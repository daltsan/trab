#!/usr/bin/env bash
# Demonstracao de escalabilidade, distribuicao de carga e tolerancia a falhas
# sobre um cluster de 3 brokers (docker-compose.cluster.yml).
#
#   ./escalabilidade.sh
#
# Nao toca no docker-compose.yml nem no ensaio.sh: o cluster sobe em outro
# projeto do compose, em outra rede e em outras portas de host (9192/9194/9196,
# Kafka UI em 8081), entao roda com o ambiente ja ensaiado de pe.
#
# ATO 1 — distribuicao de carga: tres detectores no mesmo group.id contra as 3
#         particoes de bgp.updates; cada um fica com uma particao, os 388
#         eventos sao processados uma vez so no total, e matar uma instancia
#         faz o grupo rebalancear.
# ATO 2 — tolerancia a falha: lider, replicas e ISR por particao; derruba o
#         broker lider; o lider migra, o ISR encolhe, produtor e detector
#         continuam, e religar o broker devolve o ISR a 3.
#
# O QUE ISTO *NAO* DEMONSTRA: vazao. Tres brokers na mesma CPU e no mesmo disco
# competem pelos mesmos recursos — o cluster local prova replicacao, eleicao de
# lider, tolerancia a falha e distribuicao de particoes, e nada mais. Medir
# escala horizontal de vazao exigiria maquinas separadas.
set -euo pipefail
cd "$(dirname "$0")"

PY=.venv/bin/python
FIXTURE=testes/dados/hijack_youtube.jsonl
RIB=testes/dados/rib_youtube.jsonl
COMPOSE="docker compose -f docker-compose.cluster.yml"
BROKERS=localhost:9192,localhost:9194,localhost:9196   # clientes, do host
BOOT=kafka-1:19092,kafka-2:19092,kafka-3:19092         # CLI, de dentro da rede
EVENTOS=388          # eventos normalizados da fixture de 2008
ALERTAS=586          # S1=273 + S2=273 + S3=40
GRUPO="detector-escala-$$"
LOGS=$(mktemp -d /tmp/escalabilidade-bgp.XXXXXX)
BASE="$LOGS/linha_base.json"
CLI=bgp-kafka-1      # container de onde as ferramentas de linha de comando rodam
CAIDO=""             # broker derrubado pelo ato 2, religado pela limpeza
PIDS=()

limpar() {
    trap - EXIT INT TERM
    echo
    echo "== derrubando (logs em $LOGS)"
    for pid in "${PIDS[@]:-}"; do kill -- "-$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    if [ -n "$CAIDO" ]; then docker start "$CAIDO" >/dev/null 2>&1 || true; fi
    $COMPOSE down >/dev/null 2>&1 || true
}
trap limpar EXIT INT TERM

falhar() { echo; echo "FALHOU: $*" >&2; exit 1; }

# k <ferramenta.sh> <args...> — ferramenta do Kafka de dentro do cluster. O
# bootstrap lista os tres brokers, entao continua funcionando com um fora.
k() { local f=$1; shift; docker exec "$CLI" "/opt/kafka/bin/$f" --bootstrap-server "$BOOT" "$@"; }

iniciar() {  # iniciar <nome> <comando...>
    local nome=$1; shift
    setsid "$@" >"$LOGS/$nome.log" 2>&1 &
    PIDS+=($!)
    echo "   $nome (pid $!) -> $LOGS/$nome.log"
}

ate() {  # ate <segundos> '<teste>' — reavalia o teste ate dar certo
    local fim=$((SECONDS + $1)); shift
    while ((SECONDS < fim)); do eval "$@" && return 0; sleep 0.5; done
    return 1
}

grupo() { k kafka-consumer-groups.sh --describe --group "$GRUPO" 2>/dev/null || true; }
topico() { k kafka-topics.sh --describe --topic "${1:-bgp.updates}" 2>/dev/null || true; }

# Colunas de --describe de grupo: GROUP TOPIC PARTITION CURRENT-OFFSET
# LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
donos()      { grupo | awk '$2=="bgp.updates" && $7!="-" {print $7}' | sort -u; }
membros()    { donos | wc -l; }
atribuidas() { grupo | awk '$2=="bgp.updates" && $7!="-"' | wc -l; }
commitados() { grupo | awk '$2=="bgp.updates" && $4 ~ /^[0-9]+$/ {s+=$4} END{print s+0}'; }
atraso()     { grupo | awk '$2=="bgp.updates" && $6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}'; }
mensagens()  { k kafka-get-offsets.sh --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }
offsets()    { k kafka-get-offsets.sh --topic "$1" 2>/dev/null | sort -t: -k2 -n; }
# Linha de particao: Topic: x Partition: N Leader: L Replicas: a,b,c Isr: a,b,c
lider_de()   { topico | awk -v p="Partition: $1" '$0 ~ p {print $6}' | tr -d '\r'; }
isr_de()     { topico | awk -v p="Partition: $1" '$0 ~ p {print $10}' | tr ',' ' ' | wc -w; }
isr_total()  { topico | awk '/Partition: [0-9]/{print $10}' | tr ',' ' ' | wc -w; }

# ------------------------------------------------------------------ preparo

echo "== 1/10 cluster de 3 brokers"
# Estado limpo a cada execucao: as asserções contam mensagens absolutas, e um
# topico com sobra da execucao anterior faria todas falharem.
$COMPOSE down >/dev/null 2>&1 || true
rm -rf dados-cluster-1 dados-cluster-2 dados-cluster-3
# Os diretorios tem que existir antes do bind mount: criados pelo Docker eles
# saem com dono root e o broker nao consegue formatar o log.
mkdir -p dados-cluster-1 dados-cluster-2 dados-cluster-3
$COMPOSE up -d >/dev/null
ate 240 'k kafka-topics.sh --list >/dev/null 2>&1' || falhar "o cluster nao subiu em 240 s"
ate 120 '[ "$(mensagens bgp.updates)" = 0 ] && [ "$(isr_total)" = 9 ]' \
    || falhar "os topicos replicados nao ficaram prontos"
echo "   brokers em $BROKERS, Kafka UI em http://localhost:8081"

echo
echo "== 2/10 topicos replicados em tres brokers"
topico bgp.updates
rf=$(topico bgp.updates | awk '/PartitionCount/{print $8}')
[ "$rf" = "3" ] || falhar "bgp.updates com fator de replicacao $rf, esperado 3"
echo
echo "   memoria dos brokers (teto de heap: -Xmx512m cada):"
docker stats --no-stream --format '   {{.Name}}  {{.MemUsage}}  cpu {{.CPUPerc}}' \
    bgp-kafka-1 bgp-kafka-2 bgp-kafka-3 bgp-kafka-ui-cluster

# ------------------------------------------- ATO 1: distribuicao e rebalance

echo
echo "===== ATO 1 — distribuicao de carga entre tres detectores ====="

echo "== 3/10 tres detectores no mesmo group.id ($GRUPO)"
$PY produtor/produtor.py --linha-base "$RIB" --saida "$BASE" 2>/dev/null
for i in 1 2 3; do
    iniciar "detector-$i" $PY detector/detector.py --linha-base "$BASE" \
        --broker "$BROKERS" --grupo "$GRUPO" --do-inicio
done
ate 120 '[ "$(atribuidas)" = 3 ] && [ "$(membros)" = 3 ]' \
    || falhar "o grupo nao estabilizou em 3 membros com 1 particao cada"
echo
grupo
echo "   OK: 3 membros, uma particao de bgp.updates para cada"

echo
echo "== 4/10 replay de 2008 em bgp.updates"
$PY produtor/produtor.py --arquivo "$FIXTURE" --broker "$BROKERS"
pub=$(mensagens bgp.updates)
[ "$pub" = "$EVENTOS" ] || falhar "$pub eventos em bgp.updates, esperado $EVENTOS"
# A prova de que os 388 foram processados UMA vez no total, e nao uma por
# instancia: se cada detector tivesse lido o topico inteiro, bgp.alertas teria
# 3 x 586. Numero colhido do proprio Kafka, com os tres ainda no ar.
ate 180 '[ "$(mensagens bgp.alertas)" = "$ALERTAS" ]' \
    || falhar "$(mensagens bgp.alertas) alertas em bgp.alertas, esperado $ALERTAS"
sleep 5
[ "$(mensagens bgp.alertas)" = "$ALERTAS" ] \
    || falhar "$(mensagens bgp.alertas) alertas em bgp.alertas, esperado $ALERTAS"
echo "   $pub eventos -> $ALERTAS alertas em bgp.alertas (nao $((ALERTAS * 3)))"
echo
echo "   quanto cada particao recebeu (topico:particao:offset):"
offsets bgp.updates | sed 's/^/     /'
echo "   a fixture de 2008 tem 4 prefixos distintos e a chave e o prefixo, entao"
echo "   o lote cai quase todo numa particao. Chave por prefixo compra ordem por"
echo "   bloco, nao balanceamento — com trafego real (passo 10) ele aparece."
echo
grupo
echo "   o CURRENT-OFFSET atrasado em relacao ao LOG-END-OFFSET e o commit em"
echo "   lote de 100 do detector, nao evento parado: o resto commita no fim."

echo
echo "== 5/10 rebalance: kill -9 numa das tres instancias"
echo "   --- antes da queda ---"
grupo
alvo=${PIDS[1]}
echo
echo "   kill -9 no detector-2 (pid $alvo)"
kill -9 -- "-$alvo" 2>/dev/null || true
inicio=$SECONDS
ate 240 '[ "$(membros)" = 2 ] && [ "$(atribuidas)" = 3 ]' \
    || falhar "o grupo nao rebalanceou em 240 s"
rebal=$((SECONDS - inicio))
echo
echo "   --- depois da queda ($rebal s) ---"
grupo
echo "   OK: 2 membros cobrindo as 3 particoes, rebalance em $rebal s"
echo "   (o atraso e o session.timeout.ms do consumidor: a morte foi um SIGKILL,"
echo "    sem LeaveGroup. Um encerramento limpo sai do grupo na hora.)"
echo "   A particao orfa volta do ultimo offset commitado, entao o que estava"
echo "   em voo e reprocessado: a entrega e at-least-once, de proposito — D1"
echo "   agrupa por (prefixo, origem) e duplicata nao muda o resultado."

# --------------------------------------------- ATO 2: queda de um broker

echo
echo "===== ATO 2 — tolerancia a falha de um broker ====="

echo "== 6/10 lider, replicas e ISR por particao"
topico bgp.updates
lider=$(lider_de 0)
CAIDO="bgp-kafka-$lider"
echo "   particao 0 tem o broker $lider como lider -> derrubar $CAIDO"

echo
echo "== 7/10 docker stop $CAIDO"
if [ "$CLI" = "$CAIDO" ]; then
    CLI=$(for c in bgp-kafka-1 bgp-kafka-2 bgp-kafka-3; do
              [ "$c" != "$CAIDO" ] && echo "$c" && break
          done)
fi
docker stop "$CAIDO" >/dev/null
inicio=$SECONDS
ate 180 '[ -n "$(lider_de 0)" ] && [ "$(lider_de 0)" != "$lider" ] && [ "$(isr_de 0)" = 2 ]' \
    || falhar "o lider da particao 0 nao migrou / o ISR nao encolheu em 180 s"
eleicao=$((SECONDS - inicio))
echo
topico bgp.updates
echo "   OK: lider da particao 0 migrou de $lider para $(lider_de 0) em $eleicao s, ISR = 2"

echo
echo "== 8/10 produtor e detector continuam com dois brokers"
alertas_antes=$(mensagens bgp.alertas)
$PY produtor/produtor.py --arquivo "$FIXTURE" --broker "$BROKERS"
esperado=$((EVENTOS * 2))
[ "$(mensagens bgp.updates)" = "$esperado" ] \
    || falhar "$(mensagens bgp.updates) eventos em bgp.updates, esperado $esperado"
# Os dois detectores que sobraram seguem consumindo com o broker fora: pelo
# menos mais 586 alertas. "Pelo menos" porque a entrega e at-least-once e o
# rebalance do passo 5 pode ter reprocessado o que estava em voo.
alvo_alertas=$((alertas_antes + ALERTAS))
ate 240 '[ "$(mensagens bgp.alertas)" -ge "$alvo_alertas" ]' \
    || falhar "$(mensagens bgp.alertas) alertas em bgp.alertas, esperado >= $alvo_alertas"
# Nada se perdeu: o topico inteiro, inclusive o que foi escrito antes da queda,
# ainda e legivel com o broker fora do ar.
lidos=$(k kafka-console-consumer.sh --topic bgp.updates --from-beginning \
    --max-messages "$esperado" --timeout-ms 60000 2>/dev/null | wc -l)
[ "$lidos" = "$esperado" ] || falhar "li $lidos de $esperado mensagens de bgp.updates"
echo "   OK: $esperado eventos em bgp.updates, os $EVENTOS de antes da queda"
echo "       inclusive, relidos do inicio; $(mensagens bgp.alertas) alertas"
echo "       (eram $alertas_antes) — tudo com o broker $lider fora do ar"

echo
echo "== 9/10 docker start $CAIDO: o ISR volta a 3"
docker start "$CAIDO" >/dev/null
inicio=$SECONDS
ate 240 '[ "$(isr_total)" = 9 ]' \
    || falhar "o ISR nao voltou a 3 replicas nas 3 particoes em 240 s"
volta=$((SECONDS - inicio))
CAIDO=""
echo
topico bgp.updates
echo "   OK: ISR de volta a 3 nas tres particoes, $volta s depois do start"

echo
echo "== 10/10 distribuicao de carga com trafego real"
# A fixture de 2008 tem 4 prefixos e nao espalha. A amostra do RIS Live tem
# 2.015 eventos de centenas de prefixos distintos — e ai o hash da chave
# distribui de fato entre as 3 particoes.
antes=$(offsets bgp.updates | awk -F: '{print $3}' | paste -sd' ')
$PY produtor/produtor.py --arquivo testes/dados/amostra_live.jsonl --broker "$BROKERS"
echo "   offsets antes:  $antes"
echo "   offsets depois:"
offsets bgp.updates | sed 's/^/     /'
vazias=$(offsets bgp.updates | awk -F: -v a="$antes" '
    BEGIN{split(a, ini, " ")} {if ($3 - ini[$2 + 1] == 0) n++} END{print n+0}')
[ "$vazias" = 0 ] || falhar "$vazias particoes ficaram sem nenhum evento novo"
dist=$(offsets bgp.updates | awk -F: -v a="$antes" '
    BEGIN{split(a, ini, " ")} {printf "%s%d", (NR > 1 ? "/" : ""), $3 - ini[$2 + 1]}')
echo "   OK: as 3 particoes receberam eventos ($dist) — 3 detectores dividiriam a carga"

# ------------------------------------------------------------------ resumo

echo
echo "===== resumo ====="
echo "   3 detectores, 1 particao cada: $EVENTOS eventos uma vez so -> $ALERTAS alertas"
echo "   rebalance apos SIGKILL de uma instancia: $rebal s"
echo "   eleicao de novo lider apos a queda do broker: $eleicao s"
echo "   ISR de volta a 3 apos religar o broker: $volta s"
echo "   2.015 eventos do RIS Live sobre as 3 particoes: $dist"
echo "   provado: replicacao, eleicao de lider, tolerancia a falha, distribuicao de particoes"
echo "   NAO provado: vazao horizontal — 3 brokers na mesma CPU e no mesmo disco"
echo "                competem entre si; isso exigiria maquinas separadas"
echo
echo "   Kafka UI do cluster: http://localhost:8081  (tres brokers na aba Brokers)"
echo "   logs: $LOGS"
echo
echo "Enter ou Ctrl-C derruba o cluster."
read -r || true
