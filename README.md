# Monitoramento BGP orientado a eventos

Projeto 1 de Sistemas Orientados a Eventos — UFES, entrega em 28/09/2026.

Sistema de monitoramento em tempo real da tabela de roteamento da Internet, construído sobre
Apache Kafka. Consome anúncios e retiradas de rotas BGP, detecta sequestro de prefixo, anúncio
anômalo e instabilidade de rota, e republica no broker os eventos derivados que só fazem sentido
depois de correlacionar observações de coletores independentes numa janela de tempo.

O desenho completo — arquitetura, contratos de evento, situações de interesse S1/S2/S3 e D1/D2,
riscos e decisões — está em [`PLANO.md`](PLANO.md). As regras de desenvolvimento (TDD, função
pura separada da E/S, fixtures reais) estão em [`CLAUDE.md`](CLAUDE.md).

## Requisitos

- Docker e Docker Compose (Kafka em modo KRaft, sem Zookeeper)
- Python 3.12 ou mais novo, com [`uv`](https://docs.astral.sh/uv/)
- Java 21, só para o derivador da fase 4 (build por `./gradlew`, sem instalação global)

## Preparo

```bash
uv sync                  # cria a .venv e instala as dependências
docker compose up -d     # sobe o Kafka e cria bgp.updates, bgp.alertas e bgp.derivados
```

O Kafka UI fica em <http://localhost:8080> e o broker em `localhost:9092`.

## Testes

```bash
.venv/bin/pytest                # unidade: sem rede, sem broker, roda sempre
.venv/bin/pytest -m integracao  # exige o docker compose no ar
./gradlew test                  # derivador Java (fase 4)
```

A lógica de decisão é função pura e os testes de unidade são dicionário entrando e lista saindo.
Quem precisa de broker leva o marcador `integracao` e fica fora da execução padrão.

## Produtor

Lê eventos BGP de uma fonte, normaliza para o contrato do `PLANO.md` e publica em `bgp.updates`
com o prefixo como chave.

```bash
# demonstração: republica a fixture do sequestro de 2008, sem tocar a rede
.venv/bin/python produtor/produtor.py --arquivo testes/dados/hijack_youtube.jsonl

# mesma coisa em ritmo de relógio, 60x mais rápido que o tempo real
.venv/bin/python produtor/produtor.py --arquivo testes/dados/hijack_youtube.jsonl --velocidade 60

# fluxo ao vivo do RIPE RIS
.venv/bin/python produtor/produtor.py --live --coletores rrc00,rrc12

# replay histórico: baixa os MRT do RIS na hora
.venv/bin/python produtor/produtor.py --replay 2008-02-24T18:45 2008-02-24T20:30 \
    --prefixo 208.65.152.0/22

# linha de base prefixo -> AS legítimo, a partir de um dump de RIB
.venv/bin/python produtor/produtor.py --linha-base testes/dados/rib_youtube.jsonl \
    --saida produtor/linha_base.json
```

`--arquivo` é o caminho da apresentação: roda sem internet e sem depender de coletor no ar.
`--replay` existe para mostrar que a fixture não é maquiagem — ele busca os mesmos dados na
fonte original.

## Fixtures

Os dados de teste são eventos BGP reais capturados em arquivo, nunca inventados à mão.

| Arquivo | Conteúdo |
|---|---|
| `testes/dados/amostra_live.jsonl` | 2.015 elems do RIS Live, 19 coletores, com IPv6 e retiradas |
| `testes/dados/hijack_youtube.jsonl` | 24/02/2008, Pakistan Telecom contra o YouTube, 6 coletores independentes |
| `testes/dados/rib_youtube.jsonl` | dump de RIB anterior ao sequestro, linha de base legítima |

Recapturar ou ampliar:

```bash
.venv/bin/python ferramentas/capturar.py --verificar   # confere as invariantes das fixtures
.venv/bin/python ferramentas/capturar.py live testes/dados/amostra_live.jsonl --n 2000
.venv/bin/python ferramentas/capturar.py bgp testes/dados/hijack_youtube.jsonl \
    --coletores rrc00,rrc01,rrc03,rrc12,route-views2,route-views.linx \
    --de "2008-02-24 18:45:00" --ate "2008-02-24 20:30:00" --prefixo 208.65.152.0/22
```

O modo `bgp` depende de `pybgpstream`, que por sua vez exige a biblioteca C libBGPStream
instalada no sistema (no Arch, `yay -S bgpstream`, que pede senha de sudo num terminal de
verdade). Por isso ele fica num grupo separado, fora do `uv sync` padrão:

```bash
uv sync --group captura
```

Os modos `live` e `mrt` não dependem dessa biblioteca, e o produtor também não — só a captura
multi-coletor usa esse caminho.

## O caso de aceitação

Em 24 de fevereiro de 2008 a Pakistan Telecom (AS 17557) anunciou `208.65.153.0/24`, mais
específico que o `208.65.152.0/22` legítimo do YouTube (AS 36561), e derrubou o serviço no mundo
inteiro por cerca de duas horas. Alimentado com essa fixture, o pipeline tem que acender S1
(origem inesperada), S2 (sub-prefixo mais específico) e D1 (sequestro confirmado por três
coletores independentes). Enquanto esse caminho não fechar ponta a ponta, o projeto não está
pronto.

## Estado

| Fase | Situação |
|---|---|
| 1 · Infraestrutura Kafka | pronta |
| 2 · Produtor | pronta |
| 3 · Detector (S1, S2, S3) | a fazer |
| 4 · Derivador Java (D1, D2) | a fazer |
| 5 · Painel | a fazer |
| 6 · Ensaio da apresentação | a fazer |
