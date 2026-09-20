#!/usr/bin/env python3
"""Captura fixtures de eventos BGP crus, um JSON por linha.

Casca de E/S descartavel: baixa/le a fonte, traduz para o schema de linha
e grava. Nenhuma decisao de dominio mora aqui — normalizar() da fase 2.2
e quem interpreta o que foi gravado.

Schema da linha (igual para todas as fontes):

    {"coletor": "rrc00", "projeto": "ris", "tempo": 1203878400.0,
     "tipo_elem": "A", "peer_asn": 3277,
     "campos": {"prefix": "...", "as-path": "...", "next-hop": "...",
                "communities": [{"asn": 174, "value": 21100}]}}

tipo_elem segue a convencao do pybgpstream: R (RIB), A (anuncio),
W (retirada), S (mudanca de estado de peer).

Uso:

    capturar.py live  ARQUIVO --n 2000
    capturar.py mrt   ARQUIVO --coletor rrc00 [--prefixo 208.65.152.0/22] FONTE...
    capturar.py bgp   ARQUIVO --coletores rrc00,rrc03 --de "2008-02-24 18:45:00" \
                              --ate "2008-02-24 20:30:00" --prefixo 208.65.152.0/22
    capturar.py --verificar
"""

import argparse
import ipaddress
import json
import sys
import urllib.request
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DADOS = RAIZ / "testes" / "dados"

RIS_LIVE = "https://ris-live.ripe.net/v1/stream/?format=json&client=soe-ufes-capturar"


# ---------------------------------------------------------------- utilidades

def _caminho_como_texto(segmentos):
    """Serializa AS_PATH no formato do pybgpstream: AS_SET vira {a,b}."""
    partes = []
    for seg in segmentos:
        if isinstance(seg, (list, tuple, set)):
            partes.append("{" + ",".join(str(a) for a in seg) + "}")
        else:
            partes.append(str(seg))
    return " ".join(partes)


def _linha(coletor, projeto, tempo, tipo_elem, peer_asn, campos):
    return json.dumps(
        {
            "coletor": coletor,
            "projeto": projeto,
            "tempo": tempo,
            "tipo_elem": tipo_elem,
            "peer_asn": peer_asn,
            "campos": campos,
        },
        separators=(",", ":"),
    )


def _dentro(prefixo_texto, rede):
    if rede is None:
        return True
    try:
        return ipaddress.ip_network(prefixo_texto, strict=False).subnet_of(rede)
    except (ValueError, TypeError):
        return False


# ------------------------------------------------------------------ RIS Live

def _elems_ris_live(limite):
    """Le o fluxo ao vivo do RIS (NDJSON sobre HTTP, so stdlib)."""
    with urllib.request.urlopen(RIS_LIVE, timeout=60) as fluxo:
        n = 0
        for bruta in fluxo:
            bruta = bruta.strip()
            if not bruta:
                continue
            d = json.loads(bruta).get("data")
            if not d:
                continue
            coletor = d.get("host", "").split(".")[0]
            tempo = d.get("timestamp")
            peer_asn = int(d["peer_asn"]) if d.get("peer_asn") else None
            tipo = d.get("type")

            if tipo == "RIS_PEER_STATE":
                yield _linha(coletor, "ris", tempo, "S", peer_asn,
                             {"state": d.get("state")})
                n += 1
            elif tipo == "UPDATE":
                comunidades = [{"asn": a, "value": v}
                               for a, v in d.get("community") or []]
                caminho = _caminho_como_texto(d.get("path") or [])
                for anuncio in d.get("announcements") or []:
                    for prefixo in anuncio.get("prefixes") or []:
                        yield _linha(coletor, "ris", tempo, "A", peer_asn, {
                            "prefix": prefixo,
                            "as-path": caminho,
                            "next-hop": anuncio.get("next_hop"),
                            "communities": comunidades,
                        })
                        n += 1
                for prefixo in d.get("withdrawals") or []:
                    yield _linha(coletor, "ris", tempo, "W", peer_asn,
                                 {"prefix": prefixo})
                    n += 1
            if n >= limite:
                return


# -------------------------------------------------------------------- MRT

def _atributos(lista):
    """path_attributes -> (as-path, next-hop, communities) ja traduzidos."""
    caminho, salto, comunidades = "", None, []
    for atrib in lista:
        nome = next(iter(atrib["type"].values()))
        valor = atrib.get("value")
        if nome == "AS_PATH":
            segmentos = []
            for seg in valor:
                asns = seg["value"]
                if next(iter(seg["type"].values())) == "AS_SET":
                    segmentos.append(asns)
                else:
                    segmentos.extend(asns)
            caminho = _caminho_como_texto(segmentos)
        elif nome == "NEXT_HOP":
            salto = valor
        elif nome == "COMMUNITY":
            for c in valor:
                asn, _, val = str(c).partition(":")
                if val.isdigit() and asn.isdigit():
                    comunidades.append({"asn": int(asn), "value": int(val)})
    return caminho, salto, comunidades


def _elems_mrt(caminhos, coletor, rede):
    """Le arquivos MRT (updates BGP4MP e bview TABLE_DUMP_V2).

    ponytail: so trata NLRI IPv4 (nlri/withdrawn_routes). MP_REACH_NLRI
    (IPv6) fica de fora porque as fixtures MRT deste projeto sao o caso
    de 2008, que e IPv4; o IPv6 entra pela amostra do RIS Live. Se um dia
    precisar de IPv6 historico, tratar o atributo 14/15 aqui.
    """
    from mrtparse import Reader

    indice_peers = []
    for origem in caminhos:
        for registro in Reader(str(origem)):
            d = registro.data
            subtipo = next(iter(d["subtype"].values()))
            tempo = float(next(iter(d["timestamp"].keys())))

            if subtipo == "PEER_INDEX_TABLE":
                indice_peers = d["peer_entries"]
                continue

            if subtipo.startswith("RIB_"):
                prefixo = f"{d['prefix']}/{d['length']}"
                if not _dentro(prefixo, rede):
                    continue
                for entrada in d["rib_entries"]:
                    caminho, salto, comunidades = _atributos(entrada["path_attributes"])
                    peer = indice_peers[entrada["peer_index"]]
                    yield _linha(coletor, "ris", tempo, "R", int(peer["peer_as"]), {
                        "prefix": prefixo,
                        "as-path": caminho,
                        "next-hop": salto,
                        "communities": comunidades,
                    })
                continue

            msg = d.get("bgp_message") or {}
            if next(iter(msg.get("type", {"0": ""}).values())) != "UPDATE":
                continue
            peer_asn = int(d["peer_as"])

            for rota in msg.get("withdrawn_routes") or []:
                prefixo = f"{rota['prefix']}/{rota['length']}"
                if _dentro(prefixo, rede):
                    yield _linha(coletor, "ris", tempo, "W", peer_asn,
                                 {"prefix": prefixo})

            nlri = msg.get("nlri") or []
            if not nlri:
                continue
            caminho, salto, comunidades = _atributos(msg.get("path_attributes") or [])
            for rota in nlri:
                prefixo = f"{rota['prefix']}/{rota['length']}"
                if _dentro(prefixo, rede):
                    yield _linha(coletor, "ris", tempo, "A", peer_asn, {
                        "prefix": prefixo,
                        "as-path": caminho,
                        "next-hop": salto,
                        "communities": comunidades,
                    })


# -------------------------------------------------------------- BGPStream

def _elems_bgpstream(coletores, de, ate, prefixo, tipo_registro="updates"):
    """Le do broker da CAIDA com pybgpstream (libBGPStream 2.3.0 do sistema).

    E a unica das tres fontes que cobre varios coletores numa consulta so. Isso
    importa para o D1 da fase 4: a confirmacao do sequestro exige coletores
    independentes concordando, e um arquivo MRT cobre um coletor por vez.
    """
    import pybgpstream

    kwargs = {"from_time": de, "until_time": ate, "collectors": list(coletores),
              "record_type": tipo_registro}
    if prefixo:
        kwargs["filter"] = f"prefix more {prefixo}"

    for elem in pybgpstream.BGPStream(**kwargs):
        campos = {k: v for k, v in elem.fields.items()}
        comunidades = campos.pop("communities", None)
        if comunidades is not None:
            # pybgpstream entrega um set de "asn:valor"; o schema da fixture usa
            # a forma do RIS Live, {"asn": int, "value": int}, e set nao e JSON.
            pares = []
            for c in comunidades:
                asn, _, valor = str(c).partition(":")
                if asn.isdigit() and valor.isdigit():
                    pares.append({"asn": int(asn), "value": int(valor)})
            campos["communities"] = sorted(pares, key=lambda c: (c["asn"], c["value"]))
        yield _linha(elem.record.collector, elem.record.project,
                     float(elem.record.time), elem.type, elem.peer_asn, campos)


# -------------------------------------------------------------- verificacao

def verificar():
    """Prova que as tres fixtures tem o que a fase 2.2 vai precisar."""
    def carregar(nome):
        with open(DADOS / nome, encoding="utf-8") as f:
            linhas = [json.loads(linha) for linha in f if linha.strip()]
        assert linhas, f"{nome} esta vazio"
        return linhas

    vivo = carregar("amostra_live.jsonl")
    assert any(":" in e["campos"].get("prefix", "") for e in vivo), \
        "amostra_live.jsonl nao tem nenhum prefixo IPv6"
    assert any(e["tipo_elem"] == "W" for e in vivo), \
        "amostra_live.jsonl nao tem nenhuma retirada"

    sequestro = carregar("hijack_youtube.jsonl")
    assert any(
        e["campos"].get("prefix") == "208.65.153.0/24"
        and e["campos"].get("as-path", "").split()[-1:] == ["17557"]
        for e in sequestro
    ), "hijack_youtube.jsonl nao tem o anuncio do 208.65.153.0/24 pela AS 17557"

    # Premissa do D1 (fase 4): o sequestro so e confirmado quando tres coletores
    # independentes concordam. Fixture de um coletor so nunca acenderia o alerta.
    coletores = {
        e["coletor"] for e in sequestro
        if e["campos"].get("prefix") == "208.65.153.0/24"
        and e["campos"].get("as-path", "").split()[-1:] == ["17557"]
    }
    assert len(coletores) >= 3, \
        f"o sequestro aparece em {len(coletores)} coletor(es) ({sorted(coletores)}), D1 exige 3"

    rib = carregar("rib_youtube.jsonl")
    origens = {
        e["campos"]["as-path"].split()[-1]
        for e in rib
        if e["tipo_elem"] == "R" and e["campos"].get("prefix") == "208.65.152.0/22"
    }
    assert origens == {"36561"}, \
        f"rib_youtube.jsonl da origem {origens or 'nenhuma'} para 208.65.152.0/22, esperado 36561"

    print(f"ok: {len(vivo)} elems ao vivo, {len(sequestro)} do sequestro "
          f"em {len(coletores)} coletores, {len(rib)} da RIB")


# --------------------------------------------------------------------- casca

def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--verificar", action="store_true",
                   help="so confere as invariantes das fixtures ja gravadas")
    p.add_argument("fonte", nargs="?", choices=["live", "mrt", "bgp"])
    p.add_argument("saida", nargs="?")
    p.add_argument("origens", nargs="*", help="arquivos MRT (modo mrt)")
    p.add_argument("--n", type=int, default=2000, help="quantos elems (modo live)")
    p.add_argument("--coletor", default="rrc00")
    p.add_argument("--coletores", help="lista separada por virgula (modo bgp)")
    p.add_argument("--de", help="inicio da janela, 'AAAA-MM-DD HH:MM:SS' (modo bgp)")
    p.add_argument("--ate", help="fim da janela (modo bgp)")
    p.add_argument("--registro", default="updates", choices=["updates", "ribs"],
                   help="tipo de registro do broker (modo bgp)")
    p.add_argument("--prefixo", help="so elems contidos neste prefixo")
    args = p.parse_args(argv)

    if args.verificar:
        verificar()
        return 0
    if not args.fonte or not args.saida:
        p.error("informe a fonte e o arquivo de saida, ou use --verificar")

    rede = ipaddress.ip_network(args.prefixo) if args.prefixo else None
    if args.fonte == "live":
        elems = _elems_ris_live(args.n)
    elif args.fonte == "bgp":
        if not (args.coletores and args.de and args.ate):
            p.error("modo bgp exige --coletores, --de e --ate")
        elems = _elems_bgpstream(args.coletores.split(","), args.de, args.ate,
                                 args.prefixo, args.registro)
    else:
        elems = _elems_mrt(args.origens, args.coletor, rede)

    saida = Path(args.saida)
    saida.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with saida.open("w", encoding="utf-8") as f:
        for linha in elems:
            f.write(linha + "\n")
            n += 1
    print(f"{n} elems em {saida}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
