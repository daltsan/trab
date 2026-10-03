#!/usr/bin/env bash
# Ensaio da demonstracao: sobe a cadeia inteira na ordem certa, republica o
# sequestro de 2008 e espera o evento derivado chegar ao painel.
#
#   ./ensaio.sh              caso de 2008, painel em http://localhost:8000
#   ./ensaio.sh --ao-vivo    RIS Live contra a linha de base do LACNIC IPv6 de hoje
#   PORTA=9000 ./ensaio.sh   outra porta para o painel
#
# Ctrl-C derruba tudo o que o script subiu (o broker fica de pe: quem subiu o
# docker compose foi voce, nao este script — `docker compose down` encerra).
set -euo pipefail
cd "$(dirname "$0")"

PORTA=${PORTA:-8000}
PY=.venv/bin/python
FIXTURE=testes/dados/hijack_youtube.jsonl
AO_VIVO=${1:-}
# No ao vivo a linha de base e a versionada (60 mil prefixos do 2800::/12, o
# espaco IPv6 do LACNIC); no replay de 2008 ela e gerada na hora a partir do
# dump de RIB do YouTube. O modo padrao e o replay, e ele nao muda.
if [ "$AO_VIVO" = "--ao-vivo" ]; then
    BASE=produtor/linha_base_ao_vivo.json
    ALVO=S1
else
    BASE=produtor/linha_base.json
    ALVO=sequestro
fi
MARCA=$$
LOGS=$(mktemp -d /tmp/ensaio-bgp.XXXXXX)
PIDS=()

limpar() {
    trap - EXIT INT TERM  # senao o EXIT reexecuta isto depois do INT/TERM
    echo
    echo "== derrubando (logs em $LOGS)"
    # setsid poe cada filho no proprio grupo de processos; o sinal negativo pega
    # tambem os netos, que e como a JVM que o gradle forka morre junto.
    for pid in "${PIDS[@]:-}"; do kill -- "-$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
}
trap limpar EXIT INT TERM

iniciar() {  # iniciar <nome> <comando...>
    local nome=$1; shift
    setsid "$@" >"$LOGS/$nome.log" 2>&1 &
    PIDS+=($!)
    echo "   $nome (pid $!) -> $LOGS/$nome.log"
}

esperar() {  # esperar <segundos> <arquivo> <padrao>
    local fim=$((SECONDS + $1))
    while ((SECONDS < fim)); do
        grep -qs -- "$3" "$2" && return 0
        sleep 0.5
    done
    return 1
}

echo "== 1/6 broker"
docker compose up -d >/dev/null
until docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 1; done
echo "   kafka pronto em localhost:9092"

echo "== 2/6 linha de base a partir do dump de RIB"
if [ "$AO_VIVO" = "--ao-vivo" ]; then
    [ -f "$BASE" ] || { echo "falta $BASE (veja o README)"; exit 1; }
    echo "   $(grep -c '":' "$BASE") prefixos em $BASE"
else
    $PY produtor/produtor.py --linha-base testes/dados/rib_youtube.jsonl --saida "$BASE"
fi

echo "== 3/6 consumidores no ar"
# Grupo novo por execucao: o detector e o painel comecam no fim dos topicos e so
# veem o que este ensaio publicar, e nao o que sobrou do ensaio anterior.
iniciar detector $PY detector/detector.py --linha-base "$BASE" \
    --grupo "detector-ensaio-$MARCA"
iniciar painel $PY painel/painel.py --porta "$PORTA" --grupo "painel-ensaio-$MARCA"
iniciar derivador bash -c 'cd derivador && export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && exec ./gradlew run --console=plain'

esperar 30 "$LOGS/painel.log" "painel em" || { echo "painel nao subiu"; exit 1; }
esperar 300 "$LOGS/derivador.log" "derivador:" || { echo "derivador nao subiu"; exit 1; }
sleep 10  # o Kafka Streams ainda precisa entrar no grupo e receber as particoes

if [ "$AO_VIVO" = "--ao-vivo" ]; then
    echo "== 4/6 RIS Live em bgp.updates (fica no ar ate o Ctrl-C)"
    iniciar produtor $PY produtor/produtor.py --live
    echo "== 5/6 esperando o primeiro S1 chegar ao painel"
else
    echo "== 4/6 replay do sequestro de 2008 em bgp.updates"
    $PY produtor/produtor.py --arquivo "$FIXTURE" --velocidade 300
    echo "== 5/6 esperando o sequestro_confirmado chegar ao painel"
fi
inicio=$SECONDS
if $PY - "$PORTA" "$ALVO" <<'FIM'
import json, sys, time, urllib.request
porta, alvo = sys.argv[1], sys.argv[2]
fim = time.monotonic() + (600 if alvo != "sequestro" else 120)
estado = {}
while time.monotonic() < fim:
    with urllib.request.urlopen(f"http://localhost:{porta}/dados", timeout=5) as r:
        estado = json.loads(r.read())
    for resumo in estado.values():
        if (resumo["sequestro_confirmado"] if alvo == "sequestro"
                else alvo in resumo["situacoes"]):
            print(f"   {resumo['prefixo']}: AS {resumo['as_suspeito']} contra a AS "
                  f"{resumo['as_legitimo']}, {len(resumo['coletores'])} coletores")
            sys.exit(0)
    time.sleep(1)
# Nao veio: diz o que chegou, em vez de so falhar. Ao vivo o S1 depende de uma
# anomalia real acontecer, e silencio e resultado legitimo — nao defeito.
total = {}
for resumo in estado.values():
    for s, n in resumo["situacoes"].items():
        total[s] = total.get(s, 0) + n
print(f"   nenhum {alvo}; chegaram {total or 'nenhum alerta'} em "
      f"{len(estado)} prefixos")
sys.exit(1)
FIM
then
    echo "   confirmado em $((SECONDS - inicio)) s"
else
    echo "   NAO chegou — veja $LOGS/derivador.log e bgp.derivados"
fi

echo "== 6/6 pronto"
echo "   painel     http://localhost:$PORTA"
echo "   Kafka UI   http://localhost:8080"
echo "   derivados  docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \\"
echo "                  --bootstrap-server localhost:9092 --topic bgp.derivados --from-beginning"
echo
echo "Ctrl-C encerra."
wait
