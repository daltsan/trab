"""Teste de integracao do produtor: fixture -> bgp.updates, com o broker no ar.

Prova duas coisas: o evento que chega no topico obedece ao contrato do PLANO.md,
e todos os eventos de um mesmo prefixo caem na MESMA particao. A segunda e a que
a janela deslizante do derivador (fase 4) depende — se a chave deixar de ser o
prefixo, o agrupamento por (prefixo, origem) se espalha entre particoes e o
resultado da janela fica errado.

    .venv/bin/pytest -m integracao
"""

import collections
import json
import time
import uuid
from pathlib import Path

import pytest
from confluent_kafka import Consumer, TopicPartition

from produtor.produtor import _de_arquivo, publicar

pytestmark = pytest.mark.integracao

BROKER = "localhost:9092"
TOPICO = "bgp.updates"
FIXTURE = Path(__file__).parent / "dados" / "hijack_youtube.jsonl"


def _consumidor():
    return Consumer({"bootstrap.servers": BROKER, "group.id": f"integracao-{uuid.uuid4()}",
                     "enable.auto.commit": False})


def test_eventos_do_arquivo_chegam_no_contrato_e_um_prefixo_fica_numa_so_particao():
    sonda = _consumidor()
    particoes = sonda.list_topics(TOPICO, timeout=15).topics[TOPICO].partitions
    inicio = [TopicPartition(TOPICO, p, sonda.get_watermark_offsets(
        TopicPartition(TOPICO, p), timeout=15)[1]) for p in particoes]
    sonda.close()

    elems = list(_de_arquivo(FIXTURE))
    enviados = publicar(iter(elems), BROKER, TOPICO)
    assert enviados == len(elems), \
        f"a fixture tem {len(elems)} elems, foram publicados {enviados}"

    consumidor = _consumidor()
    consumidor.assign(inicio)
    recebidos = []
    try:
        limite = time.monotonic() + 30
        while len(recebidos) < enviados and time.monotonic() < limite:
            msg = consumidor.poll(1.0)
            if msg is None:
                continue
            assert msg.error() is None, f"erro no consumo: {msg.error()}"
            recebidos.append(msg)
    finally:
        consumidor.close()

    assert len(recebidos) == enviados, \
        f"{len(recebidos)} de {enviados} eventos voltaram de {TOPICO} em 30 s"

    for msg in recebidos:
        evento = json.loads(msg.value())
        assert evento.keys() >= {"tipo", "prefixo", "origem_as", "as_path",
                                 "coletor", "peer_as", "timestamp"}
        assert evento["tipo"] == "anuncio"
        assert msg.key().decode() == evento["prefixo"], \
            "a chave do topico tem que ser o prefixo"

    por_prefixo = collections.defaultdict(set)
    for msg in recebidos:
        por_prefixo[msg.key().decode()].add(msg.partition())
    espalhados = {p: parts for p, parts in por_prefixo.items() if len(parts) > 1}
    assert not espalhados, f"prefixos espalhados entre particoes: {espalhados}"
    esperados = {e["campos"]["prefix"] for e in elems}
    assert set(por_prefixo) == esperados, \
        f"prefixos no topico {sorted(por_prefixo)} != os da fixture {sorted(esperados)}"
