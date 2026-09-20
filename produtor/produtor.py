#!/usr/bin/env python3
"""Produtor: le elems BGP de uma fonte, normaliza e publica em bgp.updates.

Casca de E/S. Nenhuma decisao de dominio aqui — normalizar() e linha_base()
moram em produtor/nucleo.py, e os leitores das fontes em ferramentas/capturar.py.

    produtor.py --arquivo testes/dados/hijack_youtube.jsonl [--velocidade N]
    produtor.py --live [--coletores rrc00,rrc15]
    produtor.py --replay 2008-02-24T18:00 2008-02-24T20:30 [--prefixo 208.65.152.0/22]
    produtor.py --linha-base testes/dados/rib_youtube.jsonl --saida produtor/linha_base.json

--arquivo e o caminho da demonstracao: roda as fases 3, 4 e 5 sem internet e
sem depender de coletor no ar no dia da apresentacao.
"""

import argparse
import datetime as dt
import ipaddress
import json
import sys
import time
import urllib.request
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from ferramentas.capturar import _elems_mrt, _elems_ris_live  # noqa: E402
from produtor.nucleo import linha_base, normalizar  # noqa: E402

BROKER = "localhost:9092"
TOPICO = "bgp.updates"
ARQUIVO_RIS = "https://data.ris.ripe.net/{coletor}/{ano}.{mes:02d}/updates.{dia}.{hhmm}.gz"


# ------------------------------------------------------------------- fontes

def _de_arquivo(caminho):
    with open(caminho, encoding="utf-8") as f:
        for linha in f:
            if linha.strip():
                yield json.loads(linha)


def _de_live(coletores):
    for bruta in _elems_ris_live(float("inf")):
        elem = json.loads(bruta)
        if not coletores or elem["coletor"] in coletores:
            yield elem


def _de_replay(inicio, fim, coletor, prefixo, destino):
    """Baixa os MRT de updates do RIS que cobrem [inicio, fim] e le com mrtparse.

    O RIS fecha um arquivo de updates a cada 5 minutos; o nome carrega o instante
    de abertura, entao a janela precisa comecar um arquivo antes.
    """
    rede = ipaddress.ip_network(prefixo) if prefixo else None
    destino.mkdir(parents=True, exist_ok=True)
    t = inicio - dt.timedelta(minutes=inicio.minute % 5, seconds=inicio.second)
    locais = []
    while t <= fim:
        url = ARQUIVO_RIS.format(coletor=coletor, ano=t.year, mes=t.month,
                                 dia=t.strftime("%Y%m%d"), hhmm=t.strftime("%H%M"))
        local = destino / Path(url).name
        if not local.exists():
            print(f"baixando {url}", file=sys.stderr)
            urllib.request.urlretrieve(url, local)
        locais.append(local)
        t += dt.timedelta(minutes=5)

    ini, f = inicio.timestamp(), fim.timestamp()
    for bruta in _elems_mrt(locais, coletor, rede):
        elem = json.loads(bruta)
        if ini <= elem["tempo"] <= f:
            yield elem


# -------------------------------------------------------------------- Kafka

def _entrega(erro, msg):
    if erro is not None:
        print(f"falha de entrega: {erro}", file=sys.stderr)


def publicar(elems, broker, topico, velocidade=None):
    """Normaliza e publica. velocidade=N reproduz o intervalo real dividido por N;
    sem ela, vai o mais rapido que o broker aceitar."""
    from confluent_kafka import Producer

    produtor = Producer({"bootstrap.servers": broker})
    anterior = None
    n = 0
    for elem in elems:
        evento = normalizar(elem)
        if evento is None:
            continue
        if velocidade and anterior is not None:
            espera = (evento["timestamp"] - anterior) / velocidade
            if espera > 0:
                time.sleep(min(espera, 5.0))  # ponytail: teto de 5 s por evento,
                # senao um buraco de horas na fonte trava a demonstracao.
        anterior = evento["timestamp"]
        produtor.produce(topico, key=evento["prefixo"].encode(),
                         value=json.dumps(evento).encode(), on_delivery=_entrega)
        produtor.poll(0)
        n += 1
    restantes = produtor.flush(30)
    if restantes:
        print(f"{restantes} mensagens nao confirmadas em 30 s", file=sys.stderr)
    return n


# -------------------------------------------------------------------- casca

def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    modo = p.add_mutually_exclusive_group(required=True)
    modo.add_argument("--arquivo", help="fixture .jsonl de elems crus")
    modo.add_argument("--live", action="store_true", help="RIS Live, fluxo continuo")
    modo.add_argument("--replay", nargs=2, metavar=("INICIO", "FIM"),
                      help="janela historica ISO, ex 2008-02-24T18:00")
    modo.add_argument("--linha-base", help="dump de RIB .jsonl -> prefixo/AS legitimo")
    p.add_argument("--velocidade", type=float, help="divisor do tempo real (modo arquivo)")
    p.add_argument("--coletores", help="lista separada por virgula (modo live)")
    p.add_argument("--coletor", default="rrc00", help="coletor do replay")
    p.add_argument("--prefixo", help="so elems contidos neste prefixo (modo replay)")
    p.add_argument("--saida", help="arquivo de saida da linha de base")
    p.add_argument("--broker", default=BROKER)
    p.add_argument("--topico", default=TOPICO)
    p.add_argument("--cache", default=str(RAIZ / "dados-mrt"),
                   help="onde guardar os MRT baixados no replay")
    args = p.parse_args(argv)

    if args.linha_base:
        if not args.saida:
            p.error("--linha-base exige --saida")
        base = linha_base(_de_arquivo(args.linha_base))
        saida = Path(args.saida)
        saida.parent.mkdir(parents=True, exist_ok=True)
        saida.write_text(json.dumps(base, indent=2), encoding="utf-8")
        print(f"{len(base)} prefixos na linha de base em {saida}", file=sys.stderr)
        return 0

    if args.arquivo:
        elems = _de_arquivo(args.arquivo)
    elif args.live:
        coletores = set(args.coletores.split(",")) if args.coletores else None
        elems = _de_live(coletores)
    else:
        ini, fim = (dt.datetime.fromisoformat(x).replace(tzinfo=dt.UTC)
                    for x in args.replay)
        elems = _de_replay(ini, fim, args.coletor, args.prefixo, Path(args.cache))

    n = publicar(elems, args.broker, args.topico, args.velocidade)
    print(f"{n} eventos em {args.topico}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
