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

### Duas linhas de base

| Arquivo | Origem | Prefixos | Para que serve |
|---|---|---|---|
| `produtor/linha_base.json` | `rib_youtube.jsonl`, gerado pelo `ensaio.sh` | 1 | replay de 2008 |
| `produtor/linha_base_ao_vivo.json` | RIBs de 30/09/2026, `2800::/12` | 59.973 | modo `--live` |

O valor é o **AS de origem** quando a RIB concorda e a **lista de origens** quando não concorda.
MOAS legítimo existe — anycast, multihoming, uma operadora com dois ASNs — e são 179 prefixos em
59.973 na RIB de hoje. Guardar só a origem do último peer que aparece faria cada uma das outras
virar falso positivo de S1 a cada anúncio; S1 dispara quando a origem está **fora** do conjunto.
No alerta, `as_esperado` continua sendo o AS único quando não há ambiguidade, e a lista quando há.

A faixa é o espaço IPv6 do LACNIC (`2800::/12`, que contém o `2804::/16` brasileiro), escolhida
medindo e não por palpite: numa captura de 300 mil eventos do RIS Live, **80% do fluxo é IPv6** e
o LACNIC IPv4 (`177.0.0.0/8 186.0.0.0/7`, 45 mil prefixos) não acendeu **nenhum** S1 em 300 mil
eventos; o `2800::/12` julga 94% dos eventos da sua faixa e acende S1 ao vivo. O teto de 2 MB do
arquivo versionado é o que impede levar IPv4 junto: a base IPv6 sozinha já ocupa 1,85 MB.

**São dois coletores, e isso não é excesso de zelo.** Com a RIB do `route-views2.saopaulo`
sozinha, o `2800:540:2000::/44` tinha um único peer registrando origem 27995 e o detector
acendeu 242 S1 contra a AS 6535 — vistos por 19 coletores, o bastante para o D1 confirmar um
sequestro que não existe. A RIB do `rrc00` registra o mesmo bloco por 34 peers, com a AS 6535
como origem. A união das duas guarda `[6535, 27995]` e o falso positivo some. Peer ralo na RIB é
a principal fonte de falso positivo de S1: quanto mais peers, mais MOAS legítimo a base conhece.

Regenerar (os `.jsonl` crus somam 750 MB e não são versionados — ficam fora do repositório):

```bash
# 1. dumps de RIB mais recentes dos dois coletores (a lista sai do broker da CAIDA)
curl -o /tmp/rrc00.gz  https://data.ris.ripe.net/rrc00/2026.09/bview.20260930.0800.gz
curl -o /tmp/rv2sp.bz2 \
    http://archive.routeviews.org/route-views2.saopaulo/bgpdata/2026.09/RIBS/rib.20260930.0400.bz2

# 2. RIB -> elems crus. O broker da CAIDA trava ao servir um dump de RIB inteiro,
#    então o modo bgp lê o arquivo já baixado, pela interface singlefile.
.venv/bin/python ferramentas/capturar.py bgp /tmp/rib_a.jsonl \
    --registro ribs --arquivo-rib /tmp/rrc00.gz  --prefixo "2800::/12"
.venv/bin/python ferramentas/capturar.py bgp /tmp/rib_b.jsonl \
    --registro ribs --arquivo-rib /tmp/rv2sp.bz2 --prefixo "2800::/12"

# 3. elems crus dos dois -> linha de base versionada (linha_base() une as origens)
cat /tmp/rib_a.jsonl /tmp/rib_b.jsonl > /tmp/rib_lacnic6.jsonl
.venv/bin/python produtor/produtor.py --linha-base /tmp/rib_lacnic6.jsonl \
    --saida produtor/linha_base_ao_vivo.json
```

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
.venv/bin/python painel/painel.py --linhas 40     # mais linhas na tela
```

| Rota | Conteúdo |
|---|---|
| `/` | página HTML única, CSS embutido, recarga automática a cada 2 s |
| `/dados` | o mesmo estado em JSON (é o que o teste de integração consulta) |

A tela mostra **as 20 linhas mais relevantes** (`--linhas` muda o corte); ao vivo o estado cresce
sem parar e sem o corte a página viraria rolagem. Como a ordenação põe os confirmados no topo, o
que importa nunca cai fora. O `/dados` continua devolvendo o estado inteiro.

No rodapé da página há uma legenda do que é cada situação (S1, S2, S3) e de cada marca
(`SEQUESTRO CONFIRMADO`, `ROTA INSTAVEL`) — é para quem olha a projeção sem ter lido o código.

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
./ensaio.sh              # caso de 2008, painel em http://localhost:8000
./ensaio.sh --ao-vivo    # RIS Live contra a linha de base do LACNIC IPv6
PORTA=9000 ./ensaio.sh
```

Ele espera o broker ficar de pé, gera a linha de base a partir de `rib_youtube.jsonl`, sobe
detector, painel e derivador em segundo plano (cada um com grupo novo, para não reler o ensaio
anterior), republica `hijack_youtube.jsonl` em `bgp.updates` e fica esperando o
`sequestro_confirmado` aparecer no `/dados` do painel. `Ctrl-C` derruba o que ele subiu; o broker
fica de pé, porque quem sobe o `docker compose` é você.

`--ao-vivo` troca as duas pontas e mantém o miolo: usa a `linha_base_ao_vivo.json` versionada em
vez de gerar a de 2008, põe o produtor em `--live` no lugar do `--arquivo`, e espera o primeiro
**S1** em vez do `sequestro_confirmado`. O modo padrão não muda.

Medido em 30/09/2026: o primeiro S1 chegou ao painel em **46 s** — `2800:540:2000::/44` anunciado
pela AS 6429 contra a `[6535, 27995]` da base, visto por **um** coletor só, que é exatamente o
caso que o D1 recusa confirmar. Em 10 min o painel acumulou 676 prefixos, 151 S3, 2 S1 e 645
`rota_instavel`. Ao vivo o S1 depende de uma anomalia real acontecer: silêncio é resultado
legítimo, e por isso o caminho de demonstração continua sendo o replay de 2008.

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
| `testes/dados/rib_moas.jsonl` | fatia de RIB de 30/09/2026 com 8 pares de origem MOAS reais |

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
