#!/usr/bin/env python3
"""Detector: le bgp.updates, aplica S1/S2/S3 e publica em bgp.alertas.

Casca de E/S. Nenhuma decisao de dominio aqui — detectar() e esperado() moram
em detector/nucleo.py.

    detector.py [--linha-base produtor/linha_base.json] [--do-inicio] [--limite N]

A linha de base vem de `produtor.py --linha-base`, que gera o mapa prefixo -> AS
legitimo a partir de um dump de RIB. Sem ela o detector fica quieto: prefixo que
nao esta na base (nem por supernet) nao tem origem esperada, logo nao tem desvio.
"""

import argparse
import collections
import json
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from detector.nucleo import detectar  # noqa: E402

BROKER = "localhost:9092"
ENTRADA = "bgp.updates"
SAIDA = "bgp.alertas"
GRUPO = "detector"
BASE = RAIZ / "produtor" / "linha_base.json"
LOTE = 100        # eventos entre um flush+commit e o proximo
OCIOSO = 30.0     # segundos sem mensagem que encerram uma execucao com --limite


def _entrega(erro, msg):
    if erro is not None:
        print(f"falha de entrega: {erro}", file=sys.stderr)


def processar(consumidor, produtor, base, saida, limite=None):
    """Consome, detecta e publica ate --limite eventos. Devolve a contagem por
    situacao.

    O commit do offset so acontece depois do flush dos alertas: se o processo
    cair entre os dois, o evento e reprocessado e o alerta sai duplicado — o que
    a fase 4 tolera, porque agrupa por (prefixo, origem). Na ordem inversa uma
    queda perderia o alerta de vez.
    """
    contagem = collections.Counter()
    n = 0
    ultima = time.monotonic()
    while limite is None or n < limite:
        try:
            msg = consumidor.poll(1.0)
        except KeyboardInterrupt:
            break  # Ctrl-C cai no flush+commit la embaixo, sem perder alerta
        if msg is None:
            if limite is not None and time.monotonic() - ultima > OCIOSO:
                break
            continue
        if msg.error() is not None:
            print(f"erro no consumo: {msg.error()}", file=sys.stderr)
            continue
        ultima = time.monotonic()
        for alerta in detectar(json.loads(msg.value()), base):
            produtor.produce(saida, key=alerta["prefixo"].encode(),
                             value=json.dumps(alerta).encode(), on_delivery=_entrega)
            contagem[alerta["situacao"]] += 1
        produtor.poll(0)
        n += 1
        if n % LOTE == 0:
            produtor.flush(30)
            consumidor.commit(asynchronous=False)
    produtor.flush(30)
    if n:
        consumidor.commit(asynchronous=False)
    contagem["eventos"] = n
    return contagem


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--linha-base", default=str(BASE),
                   help="JSON prefixo -> AS legitimo gerado por produtor.py --linha-base")
    p.add_argument("--do-inicio", action="store_true",
                   help="grupo novo comeca no inicio do topico, e nao no fim")
    p.add_argument("--limite", type=int, help="para depois de N eventos")
    p.add_argument("--broker", default=BROKER)
    p.add_argument("--entrada", default=ENTRADA)
    p.add_argument("--saida", default=SAIDA)
    p.add_argument("--grupo", default=GRUPO)
    args = p.parse_args(argv)

    from confluent_kafka import Consumer, Producer

    base = json.loads(Path(args.linha_base).read_text(encoding="utf-8"))
    consumidor = Consumer({
        "bootstrap.servers": args.broker,
        "group.id": args.grupo,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest" if args.do_inicio else "latest",
    })
    consumidor.subscribe([args.entrada])
    produtor = Producer({"bootstrap.servers": args.broker})
    try:
        contagem = processar(consumidor, produtor, base, args.saida, args.limite)
    finally:
        consumidor.close()

    print(f"{contagem.pop('eventos', 0)} eventos de {args.entrada}, "
          f"{sum(contagem.values())} alertas em {args.saida}: "
          + (", ".join(f"{s}={contagem[s]}" for s in sorted(contagem)) or "nenhum"),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
