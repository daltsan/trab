"""Nucleo puro do produtor: elem BGP cru -> evento primitivo do contrato.

Sem rede, sem Kafka, sem print, sem time.time(), sem estado global. Entra
dicionario, sai dicionario. O schema de entrada e o que ferramentas/capturar.py
grava (tipo_elem R/A/W/S, campos no vocabulario do pybgpstream).
"""

from collections.abc import Iterable

TIPO = {"A": "anuncio", "W": "retirada"}


def _caminho(texto):
    """"3277 1273 {17557,36561}" -> [3277, 1273, [17557, 36561]].

    AS_SET vira lista aninhada em vez de estourar em int(). Raro hoje (RFC 6472),
    mas um int() cru derrubaria o produtor ao vivo no dia em que aparecer.
    """
    caminho = []
    for token in (texto or "").split():
        if token.startswith("{"):
            caminho.append([int(a) for a in token.strip("{}").split(",") if a])
        else:
            caminho.append(int(token))
    return caminho


def normalizar(bruto: dict) -> dict | None:
    """Evento primitivo, ou None para o que nao vira evento (estado de peer,
    registro de RIB, elem sem prefixo)."""
    tipo = TIPO.get(bruto.get("tipo_elem"))
    prefixo = (bruto.get("campos") or {}).get("prefix")
    if tipo is None or not prefixo:
        return None

    if tipo == "retirada":
        return {"tipo": tipo, "prefixo": prefixo, "coletor": bruto.get("coletor"),
                "peer_as": bruto.get("peer_asn"), "timestamp": bruto.get("tempo")}

    caminho = _caminho(bruto["campos"].get("as-path"))
    origem = caminho[-1] if caminho else None
    evento = {"tipo": tipo, "prefixo": prefixo}
    if isinstance(origem, list):
        # Origem ambigua: nao inventa um AS unico, senao a S1 dispara alerta falso.
        evento["origem_as"] = None
        evento["as_set"] = origem
    else:
        evento["origem_as"] = origem
    evento["as_path"] = caminho
    evento["coletor"] = bruto.get("coletor")
    evento["peer_as"] = bruto.get("peer_asn")
    evento["timestamp"] = bruto.get("tempo")
    return evento


def linha_base(registros_rib: Iterable[dict]) -> dict[str, int]:
    """Dump de RIB -> mapa prefixo -> AS de origem legitimo (base de S1 e S2)."""
    base = {}
    for elem in registros_rib:
        if elem.get("tipo_elem") != "R":
            continue
        prefixo = (elem.get("campos") or {}).get("prefix")
        caminho = _caminho(elem["campos"].get("as-path"))
        origem = caminho[-1] if caminho else None
        if prefixo and isinstance(origem, int):
            base[prefixo] = origem
    return base
