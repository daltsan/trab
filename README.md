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
cd derivador && ./gradlew test  # derivador Java — o wrapper vive dentro de derivador/
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

## Detector

Consome `bgp.updates`, aplica as situações S1, S2 e S3 sobre cada evento e publica os alertas em
`bgp.alertas`, com o prefixo como chave.

```bash
# a linha de base prefixo -> AS legítimo tem que existir antes (o detector lê esse JSON)
.venv/bin/python produtor/produtor.py --linha-base testes/dados/rib_youtube.jsonl \
    --saida produtor/linha_base.json

# fica no ar consumindo o que chegar em bgp.updates
.venv/bin/python detector/detector.py

# lê o tópico desde o início e para depois de N eventos (útil para conferir uma demonstração)
.venv/bin/python detector/detector.py --do-inicio --limite 400
```

No fim da execução ele imprime no `stderr` a contagem por situação. Alimentado com a fixture de
2008 o resultado é `S1=273, S2=273, S3=40`: o `/24` sequestrado pela AS 17557 acende S1 e S2 em
cada um dos 273 anúncios, os dois `/25` do próprio YouTube acendem S3 por serem mais específicos
que `/24`, e o contra-anúncio do dono no `/24` não acende nada.

| Situação | Quando dispara | Severidade |
|---|---|---|
| S1 · origem inesperada | a origem do anúncio difere da esperada na linha de base | média |
| S2 · sub-prefixo | S1 e o prefixo é mais específico que o da linha de base | alta |
| S3 · caminho inválido | laço no `AS_PATH`, prefixo bogon, ou mais específico que `/24` (`/48` em IPv6) | média |

A origem esperada vem do prefixo exato na linha de base ou, na falta dele, do prefixo mais
específico que o cobre — o `/24` sequestrado em 2008 só é julgado porque a RIB tem o `/22` que o
contém. Prefixo fora desse espaço fica quieto, que é o que mantém o modo `--live` silencioso.

## Derivador

Aplicação Kafka Streams com duas fontes. D1 consome os alertas **S1** de `bgp.alertas`, agrupa por
`(prefixo, AS suspeito)` numa janela fixa de 5 minutos e publica `sequestro_confirmado` em
`bgp.derivados` quando três coletores distintos concordam. D2 consome `bgp.updates`, agrupa por
`(prefixo, peer_as)` na mesma janela e publica `rota_instavel` a partir de 4 alternâncias
anúncio↔retirada. Nos dois casos a chave da saída é o prefixo.

O tempo da janela vem do campo `timestamp` do evento, nunca do relógio: sem isso o replay de 2008
cairia inteiro numa janela de "agora".

```bash
cd derivador

./gradlew test                  # 13 testes em memória, sem broker e sem Docker
./gradlew run                   # fica no ar: bgp.alertas + bgp.updates -> bgp.derivados
KAFKA_BOOTSTRAP=outro:9092 ./gradlew run
```

| Evento derivado | Quando dispara | Campos próprios |
|---|---|---|
| `sequestro_confirmado` | 3 coletores distintos veem o mesmo S1 na janela | `coletores`, `confianca`, `janela_inicio`, `janela_fim` |
| `rota_instavel` | 4 alternâncias anúncio↔retirada do mesmo `(prefixo, peer_as)` | `peer_as`, `alternancias`, `janela_inicio`, `janela_fim` |

`confianca = 1 - 1/n_coletores`, em duas casas: 0,67 com três coletores. A emissão é uma por
janela, no instante em que o limiar é cruzado — o alerta chega durante o incidente, não cinco
minutos depois.

O mesmo dá para ver no Kafka UI em <http://localhost:8080>, que é o caminho da apresentação.
Esperado: `sequestro_confirmado` do `208.65.153.0/24`, `as_suspeito` 17557, `as_legitimo` 36561.

## Painel e consumidor de ações

Consome `bgp.alertas` e `bgp.derivados` numa thread e serve o estado em HTTP na outra. O estado é
acumulado **por prefixo**: contagem por situação, coletores distintos, AS suspeito e AS legítimo,
severidade máxima, primeiro e último instante, e a marca de sequestro confirmado quando o evento
derivado daquele prefixo chega.

```bash
.venv/bin/python painel/painel.py                 # http://localhost:8000
.venv/bin/python painel/painel.py --porta 9000 --do-inicio --grupo painel-2
```

| Rota | Conteúdo |
|---|---|
| `/` | página HTML única, CSS embutido, recarga automática a cada 2 s |
| `/dados` | o mesmo estado em JSON (é o que o teste de integração consulta) |

A tabela ordena confirmados primeiro, depois por severidade, depois por recência — o prefixo
sequestrado fica na primeira linha e em vermelho, que é o clímax da apresentação. A recarga é
`<meta http-equiv="refresh">` em vez de polling em JavaScript: menos código, funciona sem internet
e num projetor o efeito é o mesmo.

O **consumidor de ações é este mesmo processo**: cada evento derivado que chega imprime a
notificação no `stderr`, ao lado do painel.

```
[!] SEQUESTRO CONFIRMADO 208.65.153.0/24: AS 17557 anuncia bloco da AS 36561, 3 coletores, confianca 0.67
```

## Ensaio ponta a ponta

`ensaio.sh` sobe a demonstração inteira na ordem certa e derruba tudo no fim:

```bash
./ensaio.sh              # painel em http://localhost:8000
PORTA=9000 ./ensaio.sh
```

Ele espera o broker ficar de pé, gera a linha de base a partir de `rib_youtube.jsonl`, sobe
detector, painel e derivador em segundo plano (cada um com grupo novo, para não reler o ensaio
anterior), republica `hijack_youtube.jsonl` em `bgp.updates` e fica esperando o
`sequestro_confirmado` aparecer no `/dados` do painel. `Ctrl-C` derruba o que ele subiu; o broker
fica de pé, porque quem sobe o `docker compose` é você.

Medido em 26/09/2026, com o derivador Java no ar: o `sequestro_confirmado` do `208.65.153.0/24`
(AS 17557 contra a AS 36561) chegou ao painel **16 s** depois do produtor terminar, e o
`bgp.derivados` recebeu exatamente um evento — 271 dos 273 alertas S1 caem na mesma janela de
5 min e a emissão é uma por janela, no instante em que o terceiro coletor entra.

## Fixtures

Os dados de teste são eventos BGP reais capturados em arquivo, nunca inventados à mão.

| Arquivo | Conteúdo |
|---|---|
| `testes/dados/amostra_live.jsonl` | 2.015 elems do RIS Live, 19 coletores, com IPv6 e retiradas |
| `testes/dados/hijack_youtube.jsonl` | 24/02/2008, Pakistan Telecom contra o YouTube, 6 coletores independentes |
| `testes/dados/rib_youtube.jsonl` | dump de RIB anterior ao sequestro, linha de base legítima |
| `testes/dados/alertas_2008.jsonl` | os 586 alertas que o detector produz sobre a fixture de 2008 |
| `testes/dados/updates_live.jsonl` | os 2.015 eventos normalizados da amostra ao vivo |

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
| 3 · Detector (S1, S2, S3) | pronta |
| 4 · Derivador Java (D1, D2) | pronta |
| 5 · Painel e consumidor de ações | pronta |
| 6 · Ensaio da apresentação | a fazer |
