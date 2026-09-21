"""Nucleo puro do detector: evento primitivo + linha de base -> alertas S1/S2/S3.

Sem rede, sem Kafka, sem print, sem time.time(), sem estado global. Entra
dicionario, sai lista de dicionarios. O contrato do evento de entrada e o da
secao 2 do PLANO.md; o do alerta, o da secao 3.

    S1 · origem inesperada   origem do evento != origem esperada        media
    S2 · sub-prefixo         S1 e mais especifico que o prefixo da base  alta
    S3 · caminho invalido    laco, bogon, ou mais especifico que /24     media

S2 implica S1 e o mesmo evento gera os dois alertas: e proposital. S1 e o
sintoma ("alguem alheio anuncia isso"), S2 e o agravante ("e mais especifico,
entao vence no roteamento"). A fase 4 correlaciona S1; o painel destaca S2.

Severidade de S3 e "media", nao "alta": laco e prefixo longo sao quase sempre
erro de configuracao, nao ataque — "alta" fica reservada ao sequestro por
sub-prefixo, que e o que de fato desvia trafego.
"""

import ipaddress

TETO = {4: 24, 6: 48}  # prefixo mais longo que isto a maior parte da Internet filtra


def esperado(prefixo: str, linha_base: dict) -> tuple[int | None, str | None]:
    """AS legitimo do prefixo exato ou, na falta dele, do mais especifico que o
    cobre. Devolve (as, prefixo da base) ou (None, None).

    A RIB de 2008 tem o /22 do YouTube e nao o /24 sequestrado — sem subir para
    o supernet, S1 nunca dispararia no caso de aceitacao.
    """
    rede = ipaddress.ip_network(prefixo)
    for tamanho in range(rede.prefixlen, -1, -1):
        chave = str(rede.supernet(new_prefix=tamanho))
        if chave in linha_base:
            return linha_base[chave], chave
    return None, None


def _tem_laco(as_path) -> bool:
    """ASN repetido NAO adjacente. Repeticao adjacente e prepending legitimo:
    medidos 300 eventos so com prepending na amostra ao vivo, alertar neles
    afogaria o painel em ruido."""
    ultimo = {}
    # AS_SET vira lista aninhada; um conjunto nao tem ordem, entao fica de fora.
    for i, asn in enumerate(a for a in as_path if isinstance(a, int)):
        if i - ultimo.get(asn, i) > 1:
            return True
        ultimo[asn] = i
    return False


def _caminho_invalido(evento: dict) -> str | None:
    """Motivo de S3, ou None. Primeiro motivo vence.

    ponytail: um evento com dois defeitos (bogon E mais especifico) sai com um
    alerta so. Se a contabilizacao por AS precisar dos dois, trocar por lista.
    """
    if _tem_laco(evento.get("as_path") or []):
        return "laco_no_as_path"
    rede = ipaddress.ip_network(evento["prefixo"])
    if (rede.is_private or rede.is_reserved or rede.is_loopback
            or rede.is_link_local or rede.is_multicast):
        return "prefixo_bogon"
    if rede.prefixlen > TETO[rede.version]:
        return f"mais_especifico_que_{TETO[rede.version]}"
    return None


def _alerta(evento, situacao, severidade, motivo, **extra) -> dict:
    return {"situacao": situacao, "severidade": severidade,
            "prefixo": evento["prefixo"], "origem_as": evento.get("origem_as"),
            **extra,
            "as_path": evento.get("as_path"), "coletor": evento.get("coletor"),
            "peer_as": evento.get("peer_as"), "timestamp": evento.get("timestamp"),
            "motivo": motivo}


def detectar(evento: dict, linha_base: dict) -> list[dict]:
    """Alertas disparados por um evento primitivo. Lista vazia e o caso comum."""
    if evento.get("tipo") != "anuncio":
        return []  # retirada nao tem origem nem caminho para julgar

    alertas = []
    origem = evento.get("origem_as")
    as_legitimo, prefixo_base = esperado(evento["prefixo"], linha_base)

    # origem None e AS_SET: origem ambigua nao vira S1, seria falso positivo.
    # as_legitimo None e prefixo fora da linha de base: sem esperado, sem desvio
    # — e o que mantem o modo --live quieto em vez de gritar sobre a Internet.
    if origem is not None and as_legitimo is not None and origem != as_legitimo:
        comum = {"as_esperado": as_legitimo, "prefixo_base": prefixo_base}
        alertas.append(_alerta(evento, "S1", "media",
                               f"origem {origem} difere da esperada {as_legitimo}",
                               **comum))
        if (ipaddress.ip_network(evento["prefixo"]).prefixlen
                > ipaddress.ip_network(prefixo_base).prefixlen):
            alertas.append(_alerta(
                evento, "S2", "alta",
                f"{evento['prefixo']} e mais especifico que {prefixo_base} da base",
                **comum))

    motivo = _caminho_invalido(evento)
    if motivo:
        alertas.append(_alerta(evento, "S3", "media", motivo))
    return alertas
