"""Testes do nucleo puro do produtor: elem cru -> evento primitivo do contrato.

Alimentados pelas fixtures reais em testes/dados/. Sem rede, sem Kafka.
"""

import json
from pathlib import Path

from produtor.nucleo import linha_base, normalizar

DADOS = Path(__file__).parent / "dados"


def carregar(nome):
    with open(DADOS / nome, encoding="utf-8") as f:
        return [json.loads(linha) for linha in f if linha.strip()]


def primeiro(elems, **criterio):
    for e in elems:
        if all(e[k] == v for k, v in criterio.items()):
            return e
    raise AssertionError(f"nenhum elem com {criterio}")


def test_anuncio_vira_evento_com_os_campos_do_contrato():
    bruto = primeiro(carregar("hijack_youtube.jsonl"), tipo_elem="A",
                     coletor="rrc00", peer_asn=3333)
    assert normalizar(bruto) == {
        "tipo": "anuncio",
        "prefixo": "208.65.153.0/24",
        "origem_as": 17557,
        "as_path": [3333, 12859, 6461, 3491, 17557],
        "coletor": "rrc00",
        "peer_as": 3333,
        "timestamp": 1203878877.0,
    }


def test_retirada_nao_traz_as_path_nem_origem_as():
    bruto = primeiro(carregar("amostra_live.jsonl"), tipo_elem="W")
    evento = normalizar(bruto)
    assert evento["tipo"] == "retirada"
    assert evento["prefixo"] == bruto["campos"]["prefix"]
    assert "as_path" not in evento
    assert "origem_as" not in evento


def test_prefixo_ipv6_sobrevive_a_normalizacao():
    ipv6 = [
        e for e in carregar("amostra_live.jsonl")
        if ":" in e["campos"].get("prefix", "") and e["tipo_elem"] == "A"
    ]
    evento = normalizar(ipv6[0])
    assert evento["prefixo"] == ipv6[0]["campos"]["prefix"]
    assert ":" in evento["prefixo"]
    assert isinstance(evento["origem_as"], int)


def test_as_path_com_as_set_gera_origem_nula_e_conjunto():
    # Excecao consciente a regra de fixture real do CLAUDE.md: nenhuma das tres
    # fixtures tem AS_SET (medido: 60.000 updates ao vivo e 15.035 elems de 2008,
    # zero ocorrencias — a RFC 6472 desaconselhou AS_SET em 2009). O ramo existe
    # como guarda contra crash: int("{a,b}") estoura e derrubaria o produtor ao
    # vivo. Entrada sintetica minima, so para cobrir a guarda.
    bruto = {
        "coletor": "rrc00", "projeto": "ris", "tempo": 1203878877.0,
        "tipo_elem": "A", "peer_asn": 3333,
        "campos": {"prefix": "208.65.153.0/24", "as-path": "3333 1273 {17557,36561}"},
    }
    evento = normalizar(bruto)
    assert evento["origem_as"] is None
    assert evento["as_set"] == [17557, 36561]


def test_elem_sem_prefixo_e_descartado():
    bruto = primeiro(carregar("hijack_youtube.jsonl"), tipo_elem="A",
                     coletor="rrc00", peer_asn=3333)
    bruto["campos"].pop("prefix")
    assert normalizar(bruto) is None


def test_registro_de_estado_de_peer_nao_vira_evento():
    # Mesmo motivo do AS_SET: as fixtures gravadas nao tem elem "S" (o capturar.py
    # emite um por RIS_PEER_STATE, mas nenhum caiu na janela amostrada). Entrada
    # sintetica no schema que o proprio capturar.py escreve.
    bruto = {
        "coletor": "rrc21", "projeto": "ris", "tempo": 1789933656.46,
        "tipo_elem": "S", "peer_asn": 28634, "campos": {"state": "down"},
    }
    assert normalizar(bruto) is None


def test_linha_base_do_rib_mapeia_prefixo_para_o_as_de_origem():
    base = linha_base(carregar("rib_youtube.jsonl"))
    assert base["208.65.152.0/22"] == 36561


def test_linha_base_ignora_elem_que_nao_e_registro_de_rib():
    assert linha_base(carregar("hijack_youtube.jsonl")) == {}


def test_caso_de_2008_normaliza_com_o_sequestro_e_a_linha_base_legitima():
    eventos = [e for e in map(normalizar, carregar("hijack_youtube.jsonl")) if e]
    sequestro = [
        e for e in eventos
        if e["prefixo"] == "208.65.153.0/24" and e["origem_as"] == 17557
    ]
    assert sequestro, "nenhum anuncio do /24 pela AS 17557 sobreviveu a normalizacao"
    assert linha_base(carregar("rib_youtube.jsonl")) == {"208.65.152.0/22": 36561}


def test_sequestro_de_2008_foi_visto_por_pelo_menos_tres_coletores_independentes():
    # D1 (fase 4) so confirma o sequestro quando tres coletores distintos concordam.
    # Fixture de um coletor so faria o teste de aceitacao passar sem nunca acender D1.
    eventos = [e for e in map(normalizar, carregar("hijack_youtube.jsonl")) if e]
    coletores = {
        e["coletor"] for e in eventos
        if e["prefixo"] == "208.65.153.0/24" and e["origem_as"] == 17557
    }
    assert len(coletores) >= 3, f"o sequestro aparece so em {sorted(coletores)}"
