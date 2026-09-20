"""Teste de fumaça da infraestrutura: o broker aceita e devolve um evento.

Não testa lógica de detecção — testa que o docker-compose está no ar, que o
tópico `bgp.updates` existe e que a chave da mensagem (o prefixo) sobrevive à
ida e volta pelo Kafka.
"""

import json
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

pytestmark = pytest.mark.integracao

BROKER = "localhost:9092"
TOPICO = "bgp.updates"


def test_evento_volta_do_broker_com_a_chave_prefixo_preservada():
    prefixo = "208.65.153.0/24"
    evento = {
        "tipo": "anuncio",
        "prefixo": prefixo,
        "origem_as": 17557,
        "execucao": str(uuid.uuid4()),
    }

    entregas = []
    produtor = Producer({"bootstrap.servers": BROKER})
    produtor.produce(
        TOPICO,
        key=prefixo.encode(),
        value=json.dumps(evento).encode(),
        on_delivery=lambda erro, msg: entregas.append((erro, msg)),
    )
    assert produtor.flush(15) == 0, "broker não confirmou a entrega em 15 s"
    erro, entregue = entregas[0]
    assert erro is None, f"falha de entrega: {erro}"

    consumidor = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": f"fumaca-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    consumidor.assign([TopicPartition(TOPICO, entregue.partition(), entregue.offset())])
    try:
        limite = time.monotonic() + 15
        recebida = None
        while recebida is None and time.monotonic() < limite:
            recebida = consumidor.poll(1.0)
    finally:
        consumidor.close()

    assert recebida is not None, "nada voltou de bgp.updates em 15 s"
    assert recebida.error() is None, f"erro no consumo: {recebida.error()}"
    assert recebida.key().decode() == prefixo
    assert json.loads(recebida.value()) == evento
