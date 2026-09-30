"""Teste de integracao do painel: bgp.updates -> detector -> bgp.alertas -> painel.

Prova a cadeia com o broker no ar e o painel rodando como processo de verdade:
a fixture do sequestro de 2008 entra em bgp.updates, o detector publica os
alertas, e o /dados do painel mostra o /24 sequestrado com o AS suspeito e o
legitimo. O evento derivado (que exige a JVM no ar) e da fase 6.

    .venv/bin/pytest -m integracao
"""

import json
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest
from confluent_kafka import Consumer, TopicPartition

from detector import detector
from produtor.nucleo import linha_base
from produtor.produtor import _de_arquivo, publicar

pytestmark = pytest.mark.integracao

BROKER = "localhost:9092"
RAIZ = Path(__file__).resolve().parent.parent
DADOS = Path(__file__).parent / "dados"
FIXTURE = DADOS / "hijack_youtube.jsonl"
SEQUESTRADO = "208.65.153.0/24"
ESPERA = 60.0


def _consumidor(grupo):
    return Consumer({"bootstrap.servers": BROKER, "group.id": grupo,
                     "enable.auto.commit": False})


def _fim_de(sonda, topico):
    particoes = sonda.list_topics(topico, timeout=15).topics[topico].partitions
    return [TopicPartition(topico, p, sonda.get_watermark_offsets(
        TopicPartition(topico, p), timeout=15)[1]) for p in particoes]


def _porta_livre():
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _dados(porta):
    with urllib.request.urlopen(f"http://localhost:{porta}/dados", timeout=5) as r:
        return json.loads(r.read())


def test_o_24_sequestrado_aparece_no_dados_do_painel(tmp_path):
    base = tmp_path / "linha_base.json"
    base.write_text(json.dumps(linha_base(_de_arquivo(DADOS / "rib_youtube.jsonl"))),
                    encoding="utf-8")

    # Grupo novo a cada execucao, com os offsets iniciais fixados no fim atual de
    # cada topico: detector e painel so veem o que este teste publicar.
    marca = uuid.uuid4()
    grupo_detector, grupo_painel = f"det-integracao-{marca}", f"painel-integracao-{marca}"
    sonda = _consumidor(grupo_detector)
    sonda.commit(offsets=_fim_de(sonda, "bgp.updates"), asynchronous=False)
    sonda.close()
    sonda = _consumidor(grupo_painel)
    sonda.commit(offsets=_fim_de(sonda, "bgp.alertas") + _fim_de(sonda, "bgp.derivados"),
                 asynchronous=False)
    sonda.close()

    porta = _porta_livre()
    painel = subprocess.Popen(
        [sys.executable, str(RAIZ / "painel" / "painel.py"),
         "--porta", str(porta), "--grupo", grupo_painel, "--broker", BROKER],
        cwd=RAIZ, stderr=subprocess.DEVNULL)
    try:
        limite = time.monotonic() + 30
        while True:  # espera o servidor HTTP subir
            try:
                _dados(porta)
                break
            except (urllib.error.URLError, ConnectionError, OSError):
                assert time.monotonic() < limite, "o painel nao subiu em 30 s"
                time.sleep(0.3)

        enviados = publicar(_de_arquivo(FIXTURE), BROKER, "bgp.updates")
        assert detector.main(["--linha-base", str(base), "--grupo", grupo_detector,
                              "--limite", str(enviados)]) == 0

        limite = time.monotonic() + ESPERA
        while time.monotonic() < limite:
            estado = _dados(porta)
            if estado.get(SEQUESTRADO, {}).get("as_suspeito") is not None:
                break
            time.sleep(0.5)
    finally:
        painel.terminate()
        painel.wait(10)

    resumo = estado.get(SEQUESTRADO)
    assert resumo, f"{SEQUESTRADO} nao apareceu em /dados em {ESPERA:.0f} s: {list(estado)}"
    assert resumo["as_suspeito"] == 17557
    assert resumo["as_legitimo"] == 36561
    assert resumo["severidade"] == "alta"
    assert resumo["situacoes"] == {"S1": 273, "S2": 273}
    assert len(resumo["coletores"]) == 6
