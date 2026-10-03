# Roteiro da apresentação — 19 minutos, apresentação individual

Entrega e apresentação: segunda-feira, 5 de outubro de 2026.

Slides: <https://claude.ai/artifact/Fk7bHNDiSoNsDNebvm21Se> (notas do apresentador em cada slide).

## Antes de começar

- [ ] `docker compose up -d` já rodando, tópicos criados
- [ ] Terminal aberto na raiz do projeto, fonte grande
- [ ] Navegador com duas abas: `localhost:8000` (painel) e `localhost:8080` (Kafka UI)
- [ ] `./ensaio.sh` testado nesta máquina, nesta rede (feito em 30/09; repetir na véspera)
- [ ] Um `git status` limpo, caso peçam para ver o código

## Tempo por bloco

Sozinho, sem troca de apresentador: o risco não é o silêncio entre falas, é estourar o tempo
antes da demonstração. O corte, se atrasar, é o bloco de contratos — não a demo.

| Min | Slides | Conteúdo |
|---|---|---|
| 0–2 | capa, bgp, problema | o que é BGP (AS, prefixo, anúncio); ninguém verifica; o caso de 2008 |
| 2–4 | porque-eventos, fontes | por que é problema de SOE; dados públicos medidos |
| 4–7 | arquitetura, topicos | a figura do sistema; chave = prefixo e particionamento |
| 7–10 | contratos, sequencia | os três contratos; o caminho de uma mensagem |
| 10–12 | simples, derivados | S1/S2/S3; D1 e D2 em janela, o evento composto |
| 12–14 | configuracao, serializacao | cluster KRaft, clientes, Streams, JSON |
| 14–17 | demo, resultado | `./ensaio.sh` ao vivo, números do caso |
| 17–19 | qualidade, propriedades, limites | TDD, propriedades de SOE, limitações |

O slide `bgp` é contexto para quem nunca viu o protocolo: AS e ASN, prefixo, anúncio ao vizinho, e
o fato de que qualquer AS pode anunciar qualquer bloco. Sem ele, metade da sala não acompanha o
resto. Um minuto, sem se alongar — a história de 2008 é que fixa o conceito.

Marcos de controle: aos **7 min** tem que estar na arquitetura, aos **14 min** o `./ensaio.sh`
já tem que estar rodando. Se aos 12 min ainda estiver em contratos, pular direto para a demo —
ela roda sozinha enquanto você fala, e os slides de qualidade e propriedades cabem no fim.

Perguntas depois disso. Deixar o painel na tela.

## A demonstração, passo a passo

```bash
./ensaio.sh
```

Mostrar, nesta ordem:

1. **Painel** em `localhost:8000` — `208.65.153.0/24` marcado como confirmado, AS 17557 contra AS 36561, 6 coletores.
2. **Terminal** — a linha do consumidor de ações: `[!] SEQUESTRO CONFIRMADO ...`.
3. **Kafka UI** em `localhost:8080` — as três topicos com mensagens; abrir `bgp.derivados` e mostrar o JSON do evento composto.

Se sobrar tempo: matar o detector com Ctrl-C, republicar, e mostrar que ele retoma do offset onde parou. É desacoplamento no tempo ao vivo.

### Planos B

- **Sem rede:** nada na demonstração depende de internet; tudo vem de `testes/dados/`.
- **Docker falha:** rodar `.venv/bin/pytest` e `cd derivador && ./gradlew test` — 48 testes provam a lógica sem broker.
- **Projetor ruim:** o painel tem fundo claro e fonte grande; o Kafka UI não, então é o primeiro a cortar.

## Perguntas esperadas (o PDF avisa que virão)

**Escalabilidade.** 3 partições em `bgp.updates` permitem 3 detectores no mesmo `group.id` sem tocar em código. `bgp.alertas` e `bgp.derivados` têm 1 partição porque hoje precisam de ordem total; o custo é não escalar horizontalmente ali.

**Distribuição de carga.** O hash da chave decide a partição. Chave = prefixo, então prefixos diferentes se espalham e cada prefixo fica numa partição só.

**Tolerância a falhas.** `enable.auto.commit=false` e commit só depois do `flush()`: queda reprocessa, nunca perde. O `state store` do Streams tem changelog no próprio broker, então o derivador reconstrói a janela ao reiniciar. Entrega at-least-once; duplicata não muda o resultado porque D1 agrupa por (prefixo, origem). Réplica 1 é limitação de laboratório.

**Throughput e latência.** Detector a 7.360 eventos/s num processo; fonte ao vivo a 3.773/s — duas vezes a folga. Do replay à notificação, 1 segundo. O gargalo é a janela, não o broker.

**Desacoplamento tempo/espaço.** Espaço: Python e Java só conhecem nomes de tópico. Tempo: consumidor fora do ar processa depois, do offset onde parou.

**Síncrono ou assíncrono.** Tudo assíncrono, publish/subscribe. Ninguém espera resposta; não há request/reply em lugar nenhum do sistema.

**Middleware.** O Kafka é o middleware orientado a mensagens: desacopla no espaço, no tempo e no fluxo, e ainda persiste o histórico, o que um broker de mensagens tradicional não faz.

**Por que Kafka Streams e não outro consumidor.** Janela com estado, tolerante a falha, sem escrever gerenciamento de estado à mão. E obriga a parte JVM, que é o que prova o desacoplamento entre linguagens.

**Serializadores.** JSON em UTF-8 nos dois lados; chave `String`. Sem Avro nem schema registry: o custo seria mais um serviço e geração de código nos dois toolchains, e perderíamos a leitura direta no Kafka UI.

**E se um coletor mentir?** D1 exige três coletores independentes na mesma janela. Um coletor sozinho, ou o mesmo coletor repetindo, não confirma nada — tem teste para os dois casos.
