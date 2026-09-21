#!/usr/bin/env python3
"""Gera as fixtures derivadas que os testes do derivador (fase 4) consomem.

Casca de E/S descartavel, igual a capturar.py. Nenhuma decisao de dominio mora
aqui: normalizar(), linha_base() e detectar() ja estao nos nucleos puros das
fases 2 e 3, e o lado Java nao pode normalizar nem detectar nada por conta
propria. Entao a entrada dos testes com TopologyTestDriver e a saida do Python,
gravada em arquivo.

    testes/dados/alertas_2008.jsonl   detectar() sobre hijack_youtube.jsonl
    testes/dados/updates_live.jsonl   normalizar() sobre amostra_live.jsonl

Uso:

    derivar.py              regrava as duas fixtures
    derivar.py --verificar  so confere as invariantes das fixtures ja gravadas
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from detector.nucleo import detectar  # noqa: E402
from produtor.nucleo import linha_base, normalizar  # noqa: E402
from produtor.produtor import _de_arquivo  # noqa: E402

DADOS = RAIZ / "testes" / "dados"
ALERTAS = DADOS / "alertas_2008.jsonl"
UPDATES = DADOS / "updates_live.jsonl"


def _gravar(saida, dicionarios):
    n = 0
    with saida.open("w", encoding="utf-8") as f:
        for d in dicionarios:
            f.write(json.dumps(d, separators=(",", ":")) + "\n")
            n += 1
    print(f"{n} linhas em {saida}", file=sys.stderr)
    return n


def _alertas_2008():
    base = linha_base(_de_arquivo(DADOS / "rib_youtube.jsonl"))
    for bruto in _de_arquivo(DADOS / "hijack_youtube.jsonl"):
        evento = normalizar(bruto)
        if evento is not None:
            yield from detectar(evento, base)


def _updates_live():
    for bruto in _de_arquivo(DADOS / "amostra_live.jsonl"):
        evento = normalizar(bruto)
        if evento is not None:
            yield evento


def gerar():
    _gravar(ALERTAS, _alertas_2008())
    _gravar(UPDATES, _updates_live())


# -------------------------------------------------------------- verificacao

def _alternancias(updates):
    """(prefixo, peer_as) -> quantas vezes o tipo trocou, em ordem de timestamp.

    E a contagem que o D2 (rota instavel) faz dentro da janela deslizante.

    ponytail: o RIS carimba varias mensagens com o mesmo timestamp, entao o
    empate e desfeito por tipo, so para a contagem ser deterministica. Em outra
    ordem de empate o campeao muda — se o teste do lado Java comparar numero
    exato de alternancias, tem que empatar do mesmo jeito.
    """
    por_rota = {}
    for e in sorted(updates, key=lambda e: (e["timestamp"], e["tipo"])):
        por_rota.setdefault((e["prefixo"], e["peer_as"]), []).append(e["tipo"])
    return {rota: sum(a != b for a, b in zip(tipos, tipos[1:]))
            for rota, tipos in por_rota.items()}


def verificar():
    """Prova que as duas fixtures tem o que os testes do lado Java precisam.

    Falha se alguem regerar com fonte ou linha de base erradas.
    """
    def carregar(caminho):
        linhas = list(_de_arquivo(caminho))
        assert linhas, f"{caminho.name} esta vazio"
        return linhas

    alertas = carregar(ALERTAS)
    assert len(alertas) == 586, f"alertas_2008.jsonl tem {len(alertas)} alertas, esperado 586"

    sequestro = Counter(a["situacao"] for a in alertas
                        if a["prefixo"] == "208.65.153.0/24" and a["origem_as"] == 17557)
    assert sequestro == {"S1": 273, "S2": 273}, \
        f"o 208.65.153.0/24 pela AS 17557 da {dict(sequestro)}, esperado S1=273 e S2=273"

    s3 = Counter(a["prefixo"] for a in alertas if a["situacao"] == "S3")
    assert s3 == {"208.65.153.0/25": 20, "208.65.153.128/25": 20}, \
        f"S3 saiu em {dict(s3)}, esperado 20 em cada /25 do YouTube"

    quietos = {a["prefixo"] for a in alertas} - {
        "208.65.153.0/24", "208.65.153.0/25", "208.65.153.128/25"}
    assert not quietos, f"alerta indevido em {sorted(quietos)}"
    # Os 40 S3 sao dos /25 do proprio YouTube (mais especificos que /24), mas o
    # /24 anunciado pelo dono legitimo e o /22 da base nao podem gerar nada.
    dono = [a for a in alertas
            if a["origem_as"] == 36561 and a["prefixo"] == "208.65.153.0/24"]
    assert not dono, f"{len(dono)} alerta(s) contra o /24 anunciado pela propria AS 36561"

    # Premissa do D1 (fase 4): tres coletores independentes precisam concordar.
    coletores = {a["coletor"] for a in alertas if a["situacao"] == "S1"}
    assert len(coletores) >= 3, \
        f"o sequestro aparece em {len(coletores)} coletor(es) ({sorted(coletores)}), D1 exige 3"

    updates = carregar(UPDATES)
    assert len(updates) == 2015, f"updates_live.jsonl tem {len(updates)} eventos, esperado 2015"
    retiradas = sum(e["tipo"] == "retirada" for e in updates)
    assert retiradas == 266, f"updates_live.jsonl tem {retiradas} retiradas, esperado 266"
    ipv6 = sum(":" in e["prefixo"] for e in updates)
    assert ipv6 == 649, f"updates_live.jsonl tem {ipv6} prefixos IPv6, esperado 649"

    # Premissa do D2 (fase 4): o teste de aceitacao da janela deslizante usa
    # este caso concreto de flapping. Sem ele, D2 nunca acende.
    rota, trocas = max(_alternancias(updates).items(), key=lambda kv: kv[1])
    assert (rota, trocas) == (("216.75.128.0/24", 21232), 4), \
        f"o campeao de alternancias e {rota} com {trocas}, esperado ('216.75.128.0/24', 21232) com 4"

    print(f"ok: {len(alertas)} alertas de 2008 em {len(coletores)} coletores, "
          f"{len(updates)} updates ao vivo ({retiradas} retiradas, {ipv6} IPv6), "
          f"flapping de {rota[0]} pelo peer {rota[1]} com {trocas} alternancias")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--verificar", action="store_true",
                   help="so confere as invariantes das fixtures ja gravadas")
    args = p.parse_args(argv)
    if args.verificar:
        verificar()
    else:
        gerar()
        verificar()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
