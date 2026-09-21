"""Teste de integracao do detector: bgp.updates -> detector -> bgp.alertas.

Prova a cadeia inteira com o broker no ar, sobre a fixture do sequestro de 2008:
o /24 sequestrado acende S1 e S2, os dois /25 acendem S3, o contra-anuncio do
proprio dono nao acende nada, e a chave da mensagem de alerta e o prefixo — que
e do que a janela deslizante do derivador (fase 4) depende.

    .venv/bin/pytest -m integracao
"""

import json
import time
import uuid
from pathlib import Path

import pytest
from confluent_kafka import Consumer, TopicPartition

from detector import detector
from produtor.nucleo import linha_base
from produtor.produtor import _de_arquivo, publicar

pytestmark = pytest.mark.integracao

BROKER = "localhost:9092"
ENTRADA = "bgp.updates"
SAIDA = "bgp.alertas"
DADOS = Path(__file__).parent / "dados"
FIXTURE = DADOS / "hijack_youtube.jsonl"

# Gabarito da fixture: 273 anuncios do /24 pela AS 17557 (cada um acende S1 e S2)
# e 20 + 20 anuncios dos dois /25 (cada um acende S3).
ESPERADOS = 273 + 273 + 40


def _consumidor(grupo):
    return Consumer({"bootstrap.servers": BROKER, "group.id": grupo,
                     "enable.auto.commit": False})


def _fim_de(sonda, topico):
    particoes = sonda.list_topics(topico, timeout=15).topics[topico].partitions
    return [TopicPartition(topico, p, sonda.get_watermark_offsets(
        TopicPartition(topico, p), timeout=15)[1]) for p in particoes]


def test_fixture_de_2008_vira_s1_s2_e_s3_em_bgp_alertas(tmp_path):
    base = tmp_path / "linha_base.json"
    base.write_text(json.dumps(linha_base(_de_arquivo(DADOS / "rib_youtube.jsonl"))),
                    encoding="utf-8")

    # Grupo novo a cada execucao, com o offset inicial fixado no fim atual de
    # bgp.updates: o detector le so o que este teste publicar, e nao o que
    # sobrou de outra execucao.
    grupo = f"detector-integracao-{uuid.uuid4()}"
    sonda = _consumidor(grupo)
    sonda.commit(offsets=_fim_de(sonda, ENTRADA), asynchronous=False)
    inicio_alertas = _fim_de(sonda, SAIDA)
    sonda.close()

    enviados = publicar(_de_arquivo(FIXTURE), BROKER, ENTRADA)
    assert detector.main(["--linha-base", str(base), "--grupo", grupo,
                          "--limite", str(enviados)]) == 0

    consumidor = _consumidor(f"leitor-{uuid.uuid4()}")
    consumidor.assign(inicio_alertas)
    recebidas = []
    try:
        limite = time.monotonic() + 30
        while len(recebidas) < ESPERADOS and time.monotonic() < limite:
            msg = consumidor.poll(1.0)
            if msg is None:
                continue
            assert msg.error() is None, f"erro no consumo: {msg.error()}"
            recebidas.append(msg)
    finally:
        consumidor.close()

    assert len(recebidas) == ESPERADOS, \
        f"{len(recebidas)} de {ESPERADOS} alertas voltaram de {SAIDA} em 30 s"

    alertas = [json.loads(m.value()) for m in recebidas]
    for msg, alerta in zip(recebidas, alertas, strict=True):
        assert msg.key().decode() == alerta["prefixo"], \
            "a chave do topico tem que ser o prefixo"

    s1 = [a for a in alertas if a["situacao"] == "S1"]
    assert s1 and all(a["prefixo"] == "208.65.153.0/24" and a["origem_as"] == 17557
                      and a["as_esperado"] == 36561 for a in s1)

    s2 = [a for a in alertas if a["situacao"] == "S2"]
    assert s2 and all(a["prefixo"] == "208.65.153.0/24" and a["severidade"] == "alta"
                      for a in s2)

    s3 = {a["prefixo"] for a in alertas if a["situacao"] == "S3"}
    assert s3 == {"208.65.153.0/25", "208.65.153.128/25"}

    # O negativo importa tanto quanto o positivo: o dono reanunciando o proprio
    # /24 e resposta ao sequestro, nao alerta. (Os dois /25, tambem da AS 36561,
    # acendem S3 por serem mais especificos que /24 — isso e anuncio anomalo,
    # nao sequestro, e sai la em cima no conjunto s3.)
    assert not [a for a in alertas
                if a["prefixo"] == "208.65.153.0/24" and a["origem_as"] == 36561], \
        "o contra-anuncio da AS 36561 no proprio /24 nao pode virar alerta"
