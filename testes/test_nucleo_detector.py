"""Testes do nucleo puro do detector: evento + linha de base -> alertas S1/S2/S3.

Alimentados pelas fixtures reais em testes/dados/. Sem rede, sem Kafka.
"""

import json
from pathlib import Path

from detector.nucleo import detectar, esperado
from produtor.nucleo import linha_base, normalizar

DADOS = Path(__file__).parent / "dados"


def carregar(nome):
    with open(DADOS / nome, encoding="utf-8") as f:
        return [json.loads(linha) for linha in f if linha.strip()]


def eventos(nome):
    return [e for e in map(normalizar, carregar(nome)) if e]


def base_youtube():
    return linha_base(carregar("rib_youtube.jsonl"))


def primeiro(nome, **criterio):
    """Primeiro evento normalizado que casa com o criterio."""
    for e in eventos(nome):
        if all(e.get(k) == v for k, v in criterio.items()):
            return e
    raise AssertionError(f"nenhum evento com {criterio} em {nome}")


def situacoes(alertas):
    return [a["situacao"] for a in alertas]


def test_anuncio_de_origem_ja_conhecida_nao_gera_alerta():
    evento = primeiro("hijack_youtube.jsonl", prefixo="208.65.152.0/22")
    assert evento["origem_as"] == 36561
    assert detectar(evento, base_youtube()) == []


def test_origem_inesperada_em_prefixo_coberto_gera_s1_com_o_as_legitimo():
    evento = primeiro("hijack_youtube.jsonl", prefixo="208.65.153.0/24",
                      origem_as=17557)
    s1 = [a for a in detectar(evento, base_youtube()) if a["situacao"] == "S1"]
    assert len(s1) == 1
    assert s1[0] == {
        "situacao": "S1",
        "severidade": "media",
        "prefixo": "208.65.153.0/24",
        "origem_as": 17557,
        "as_esperado": 36561,
        "prefixo_base": "208.65.152.0/22",
        "as_path": evento["as_path"],
        "coletor": evento["coletor"],
        "peer_as": evento["peer_as"],
        "timestamp": evento["timestamp"],
        "motivo": "origem 17557 difere da esperada 36561",
    }


def test_contra_anuncio_do_proprio_dono_nao_gera_alerta():
    evento = primeiro("hijack_youtube.jsonl", prefixo="208.65.153.0/24",
                      origem_as=36561)
    assert detectar(evento, base_youtube()) == []


def test_evento_com_origem_ambigua_de_as_set_nao_gera_s1():
    # Excecao consciente a regra de fixture real do CLAUDE.md, no mesmo tom do
    # teste de AS_SET do produtor: medido, nenhuma das fixtures tem AS_SET
    # (388 elems de 2008 e 2.015 ao vivo, zero ocorrencias — a RFC 6472
    # desaconselhou AS_SET em 2009). O ramo existe porque origem ambigua nao
    # pode virar S1: qualquer origem escolhida a dedo seria falso positivo.
    bruto = {
        "coletor": "rrc00", "projeto": "ris", "tempo": 1203878877.0,
        "tipo_elem": "A", "peer_asn": 3333,
        "campos": {"prefix": "208.65.153.0/24", "as-path": "3333 1273 {17557,36561}"},
    }
    evento = normalizar(bruto)
    assert evento["origem_as"] is None
    assert situacoes(detectar(evento, base_youtube())) == []


def test_prefixo_fora_da_linha_de_base_nao_gera_alerta():
    evento = primeiro("amostra_live.jsonl", tipo="anuncio", prefixo="76.7.47.0/24")
    assert esperado(evento["prefixo"], base_youtube()) == (None, None)
    assert detectar(evento, base_youtube()) == []


def test_sub_prefixo_com_outra_origem_gera_s2_de_severidade_alta():
    evento = primeiro("hijack_youtube.jsonl", prefixo="208.65.153.0/24",
                      origem_as=17557)
    alertas = detectar(evento, base_youtube())
    s2 = [a for a in alertas if a["situacao"] == "S2"]
    assert len(s2) == 1
    assert s2[0]["severidade"] == "alta"
    assert s2[0]["prefixo_base"] == "208.65.152.0/22"
    assert s2[0]["as_esperado"] == 36561
    # S2 implica S1 de proposito: o mesmo evento acende as duas.
    assert situacoes(alertas) == ["S1", "S2"]


def test_caminho_com_laco_gera_s3():
    # Laco real, medido na fixture ao vivo: 7 eventos com ASN repetido nao
    # adjacente. Este repete a AS 134840 com seis saltos de distancia.
    evento = primeiro("amostra_live.jsonl", prefixo="43.109.51.0/24")
    assert evento["as_path"] == [38001, 136168, 134840, 45558, 7473, 6461, 3356,
                                 132876, 136255, 134840, 24429]
    s3 = [a for a in detectar(evento, base_youtube()) if a["situacao"] == "S3"]
    assert len(s3) == 1
    assert s3[0]["motivo"] == "laco_no_as_path"
    assert s3[0]["severidade"] == "media"
    assert "as_esperado" not in s3[0] and "prefixo_base" not in s3[0]


def test_prepending_adjacente_nao_gera_s3():
    # 300 eventos ao vivo repetem ASN so de forma adjacente (prepending legitimo,
    # ferramenta normal de engenharia de trafego). Alertar neles afogaria o
    # painel em ruido, entao repeticao adjacente nao e laco.
    evento = primeiro("amostra_live.jsonl", tipo="anuncio", prefixo="45.229.88.0/24")
    assert evento["as_path"] == [28634, 22356, 268624, 268624, 52630, 267106]
    assert detectar(evento, base_youtube()) == []


def test_prefixo_bogon_gera_s3():
    # Excecao consciente a regra de fixture real do CLAUDE.md: medido, zero
    # prefixos bogon nos 2.403 eventos das duas fixtures — os coletores publicos
    # ja filtram espaco privado antes de exportar. O ramo existe porque um
    # anuncio de 10.0.0.0/8 em BGP publico e exatamente o erro de configuracao
    # que S3 tem que pegar, e o teste prova que ipaddress cobre o caso sem lista
    # de bogon mantida a mao.
    bruto = {
        "coletor": "rrc00", "projeto": "ris", "tempo": 1203878877.0,
        "tipo_elem": "A", "peer_asn": 3333,
        "campos": {"prefix": "10.0.0.0/8", "as-path": "3333 1273 64512"},
    }
    s3 = [a for a in detectar(normalizar(bruto), {}) if a["situacao"] == "S3"]
    assert len(s3) == 1
    assert s3[0]["motivo"] == "prefixo_bogon"


def test_prefixo_mais_especifico_que_24_gera_s3():
    evento = primeiro("hijack_youtube.jsonl", prefixo="208.65.153.0/25")
    alertas = detectar(evento, base_youtube())
    assert situacoes(alertas) == ["S3"]
    assert alertas[0]["motivo"] == "mais_especifico_que_24"


def test_retirada_nao_gera_alerta():
    evento = primeiro("amostra_live.jsonl", tipo="retirada")
    assert detectar(evento, base_youtube()) == []


def test_caso_de_2008_acende_s1_e_s2_com_17557_como_suspeita():
    base = base_youtube()
    alertas = [a for e in eventos("hijack_youtube.jsonl") for a in detectar(e, base)]

    s1 = [a for a in alertas if a["situacao"] == "S1"]
    s2 = [a for a in alertas if a["situacao"] == "S2"]
    assert s1 and s2
    assert {a["origem_as"] for a in s1} == {17557}
    assert {a["as_esperado"] for a in s1} == {36561}
    assert {a["prefixo"] for a in s1} == {"208.65.153.0/24"}
    assert {a["prefixo"] for a in s2} == {"208.65.153.0/24"}
    assert {a["severidade"] for a in s2} == {"alta"}
    # O dono anunciando o proprio bloco fica de fora, senao o alerta vira ruido.
    assert len(s1) == 273
    # Os dois /25 do proprio dono sao anuncio anomalo, nao sequestro.
    assert {a["prefixo"] for a in alertas if a["situacao"] == "S3"} == {
        "208.65.153.0/25", "208.65.153.128/25"}
    # Tres ou mais coletores independentes viram o desvio: e o que D1 exige.
    assert len({a["coletor"] for a in s1}) >= 3
