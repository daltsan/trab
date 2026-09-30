"""Nucleo puro do painel: estado acumulado + mensagem -> estado, agrupado por prefixo.

Sem rede, sem Kafka, sem print, sem time.time(), sem estado global. A mesma funcao
recebe alerta (secao 3 do PLANO.md, campo `situacao`) e evento derivado (campo
`tipo`), e decide pelo formato da mensagem — o painel consome dois topicos e nao
tem por que saber de qual veio cada uma.

O acumulo e incremental: cada mensagem mexe so na linha do proprio prefixo. O
painel e consumidor continuo, recalcular o resumo inteiro a cada mensagem seria
desperdicio.

O estado e MUTADO no lugar e devolvido, em vez de copiado: a casca chama isto
milhares de vezes por segundo. Quem le o estado de outra thread precisa de trava
(ver painel.py).
"""

SEVERIDADES = ["baixa", "media", "alta"]
DERIVADOS = ("sequestro_confirmado", "rota_instavel")


def _linha(estado: dict, prefixo: str) -> dict:
    return estado.setdefault(prefixo, {
        "prefixo": prefixo,
        "situacoes": {},
        "coletores": [],
        "as_suspeito": None,
        "as_legitimo": None,
        "severidade": "baixa",
        "primeiro": None,
        "ultimo": None,
        "sequestro_confirmado": False,
        "rota_instavel": False,
    })


def _coletores(linha: dict, novos) -> None:
    faltando = set(novos) - set(linha["coletores"])
    if faltando:
        linha["coletores"] = sorted(set(linha["coletores"]) | faltando)


def _instante(linha: dict, inicio, fim) -> None:
    if inicio is not None:
        linha["primeiro"] = inicio if linha["primeiro"] is None \
            else min(linha["primeiro"], inicio)
    if fim is not None:
        linha["ultimo"] = fim if linha["ultimo"] is None else max(linha["ultimo"], fim)


def aplicar(estado: dict, mensagem: dict) -> dict:
    """Acumula um alerta ou um evento derivado na linha do seu prefixo.

    Mensagem sem prefixo, ou de formato desconhecido, e ignorada: o painel fica
    exposto a qualquer coisa que caia no topico e nao pode morrer por causa disso.
    """
    prefixo = mensagem.get("prefixo")
    situacao = mensagem.get("situacao")
    tipo = mensagem.get("tipo")
    if not prefixo or not (situacao or tipo in DERIVADOS):
        return estado

    linha = _linha(estado, prefixo)
    if situacao:
        linha["situacoes"][situacao] = linha["situacoes"].get(situacao, 0) + 1
        _coletores(linha, [mensagem["coletor"]] if mensagem.get("coletor") else [])
        # Severidade fora da escala vira "baixa": o topico e fronteira de
        # confianca, e um alerta estranho nao pode derrubar a thread do painel.
        severidade = mensagem.get("severidade")
        linha["severidade"] = max(
            linha["severidade"], severidade if severidade in SEVERIDADES else "baixa",
            key=SEVERIDADES.index)
        # So S1 e S2 trazem a origem esperada; S3 julga o caminho, nao a origem,
        # e o `origem_as` dele costuma ser o dono legitimo do bloco.
        if mensagem.get("as_esperado") is not None:
            linha["as_suspeito"] = mensagem.get("origem_as")
            linha["as_legitimo"] = mensagem["as_esperado"]
        _instante(linha, mensagem.get("timestamp"), mensagem.get("timestamp"))
    else:
        linha[tipo] = True
        _coletores(linha, mensagem.get("coletores", []))
        if tipo == "sequestro_confirmado":
            linha["as_suspeito"] = mensagem.get("as_suspeito")
            linha["as_legitimo"] = mensagem.get("as_legitimo")
            linha["severidade"] = "alta"
        _instante(linha, mensagem.get("janela_inicio"), mensagem.get("janela_fim"))
    return estado
