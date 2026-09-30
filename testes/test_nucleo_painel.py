"""Testes do nucleo puro do painel: estado + mensagem -> estado por prefixo.

Alimentados pelos alertas reais derivados da fixture de 2008. Sem rede, sem Kafka.
"""

import json
from pathlib import Path

from painel.nucleo import aplicar

DADOS = Path(__file__).parent / "dados"
SEQUESTRADO = "208.65.153.0/24"


def carregar(nome):
    with open(DADOS / nome, encoding="utf-8") as f:
        return [json.loads(linha) for linha in f if linha.strip()]


def alertas_2008():
    return carregar("alertas_2008.jsonl")


def primeiro(alertas, **criterio):
    for a in alertas:
        if all(a.get(k) == v for k, v in criterio.items()):
            return a
    raise AssertionError(f"nenhum alerta com {criterio}")


def reduzir(mensagens, estado=None):
    for m in mensagens:
        estado = aplicar(estado if estado is not None else {}, m)
    return estado


# O evento derivado que o derivador da fase 4 publica em bgp.derivados para esta
# mesma fixture (contrato da secao 3 do PLANO.md).
DERIVADO = {
    "tipo": "sequestro_confirmado",
    "prefixo": SEQUESTRADO,
    "as_legitimo": 36561,
    "as_suspeito": 17557,
    "coletores": ["rrc00", "rrc01", "rrc03"],
    "confianca": 0.67,
    "janela_inicio": 1203878700.0,
    "janela_fim": 1203879000.0,
}


def test_alerta_simples_entra_no_resumo_do_prefixo():
    alerta = primeiro(alertas_2008(), situacao="S1")
    estado = aplicar({}, alerta)

    resumo = estado[SEQUESTRADO]
    assert resumo["situacoes"] == {"S1": 1}
    assert resumo["coletores"] == [alerta["coletor"]]
    assert resumo["as_suspeito"] == 17557
    assert resumo["as_legitimo"] == 36561
    assert resumo["severidade"] == "media"
    assert resumo["primeiro"] == resumo["ultimo"] == alerta["timestamp"]
    assert resumo["sequestro_confirmado"] is False


def test_alertas_repetidos_do_mesmo_prefixo_nao_viram_linhas_separadas():
    s1 = [a for a in alertas_2008() if a["situacao"] == "S1"]
    estado = reduzir(s1)

    assert list(estado) == [SEQUESTRADO]
    assert estado[SEQUESTRADO]["situacoes"]["S1"] == len(s1) == 273


def test_coletores_distintos_sao_acumulados_no_prefixo():
    estado = reduzir(a for a in alertas_2008() if a["prefixo"] == SEQUESTRADO)

    assert estado[SEQUESTRADO]["coletores"] == [
        "route-views.linx", "route-views2", "rrc00", "rrc01", "rrc03", "rrc12"]


def test_sequestro_confirmado_marca_o_prefixo_como_confirmado():
    estado = aplicar({}, DERIVADO)

    resumo = estado[SEQUESTRADO]
    assert resumo["sequestro_confirmado"] is True
    assert resumo["as_suspeito"] == 17557
    assert resumo["as_legitimo"] == 36561
    assert resumo["coletores"] == ["rrc00", "rrc01", "rrc03"]


def test_severidade_alta_do_s2_domina_a_media_do_s1():
    alertas = alertas_2008()
    estado = reduzir([primeiro(alertas, situacao="S2"), primeiro(alertas, situacao="S1")])

    assert estado[SEQUESTRADO]["severidade"] == "alta"


def test_rota_instavel_entra_sem_marcar_sequestro():
    instavel = {"tipo": "rota_instavel", "prefixo": "102.221.15.0/24", "peer_as": 37100,
                "alternancias": 4, "janela_inicio": 1789933500.0,
                "janela_fim": 1789933800.0}
    estado = aplicar({}, instavel)

    resumo = estado["102.221.15.0/24"]
    assert resumo["rota_instavel"] is True
    assert resumo["sequestro_confirmado"] is False


def test_estado_vazio_nao_quebra():
    assert aplicar({}, {}) == {}


def test_caso_de_2008_mostra_o_24_confirmado_com_17557_contra_36561():
    estado = reduzir(alertas_2008() + [DERIVADO])

    resumo = estado[SEQUESTRADO]
    assert len(resumo["coletores"]) == 6
    assert resumo["as_suspeito"] == 17557
    assert resumo["as_legitimo"] == 36561
    assert resumo["sequestro_confirmado"] is True
    assert resumo["severidade"] == "alta"
    assert resumo["situacoes"] == {"S1": 273, "S2": 273}
    # os dois /25 do proprio dono acendem S3 e ficam em linhas separadas
    assert set(estado) == {SEQUESTRADO, "208.65.153.0/25", "208.65.153.128/25"}


def test_severidade_desconhecida_no_topico_nao_derruba_o_painel():
    estranho = {"situacao": "S9", "severidade": "catastrofica",
                "prefixo": "203.0.113.0/24", "coletor": "rrc00", "timestamp": 1.0}
    assert aplicar({}, estranho)["203.0.113.0/24"]["severidade"] == "baixa"
