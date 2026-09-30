#!/usr/bin/env python3
"""Painel + consumidor de acoes: bgp.alertas e bgp.derivados -> tabela por prefixo.

Casca de E/S. Nenhuma decisao de dominio aqui — o acumulo por prefixo mora em
painel/nucleo.py.

    painel.py [--porta 8000] [--do-inicio] [--grupo painel]

    /       pagina HTML unica, CSS embutido, com recarga automatica a cada 2 s
    /dados  o mesmo estado em JSON (e o que o teste de integracao consulta)

O consumidor de acoes e este mesmo processo: cada evento derivado que chega
imprime uma linha de notificacao no stderr. Dois processos separados seria
cerimonia sem ganho — quem ve o painel ve o log ao lado.

A recarga e `<meta http-equiv="refresh">` e nao polling em JavaScript: menos
codigo, funciona sem internet, e num projetor o efeito e o mesmo.
"""

import argparse
import copy
import html
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from painel.nucleo import DERIVADOS, SEVERIDADES, aplicar  # noqa: E402

BROKER = "localhost:9092"
TOPICOS = ["bgp.alertas", "bgp.derivados"]
GRUPO = "painel"
PORTA = 8000

# O estado e escrito pela thread do consumidor e lido pela do servidor HTTP.
# ponytail: uma trava global para o mapa inteiro. O consumidor segura a trava por
# um dicionario de update e o servidor por uma serializacao; com um topico de um
# punhado de prefixos isso nunca e gargalo. Se for, a saida e trava por prefixo.
TRAVA = threading.Lock()
ESTADO: dict = {}


def notificar(derivado: dict) -> str:
    """Linha de notificacao do consumidor de acoes, para o stderr."""
    if derivado.get("tipo") == "sequestro_confirmado":
        return (f"[!] SEQUESTRO CONFIRMADO {derivado['prefixo']}: "
                f"AS {derivado.get('as_suspeito')} anuncia bloco da AS "
                f"{derivado.get('as_legitimo')}, {len(derivado.get('coletores', []))} "
                f"coletores, confianca {derivado.get('confianca')}")
    return (f"[!] ROTA INSTAVEL {derivado['prefixo']}: "
            f"{derivado.get('alternancias')} alternancias no peer "
            f"{derivado.get('peer_as')}")


def consumir(consumidor, parar: threading.Event) -> None:
    while not parar.is_set():
        msg = consumidor.poll(1.0)
        if msg is None:
            continue
        if msg.error() is not None:
            print(f"erro no consumo: {msg.error()}", file=sys.stderr)
            continue
        try:
            mensagem = json.loads(msg.value())
        except ValueError:
            continue  # lixo no topico nao derruba o painel
        with TRAVA:
            aplicar(ESTADO, mensagem)
        if mensagem.get("tipo") in DERIVADOS:
            print(notificar(mensagem), file=sys.stderr, flush=True)


def ordenar(estado: dict) -> list:
    """Confirmados primeiro, depois severidade, depois recencia."""
    return sorted(estado.values(),
                  key=lambda r: (not r["sequestro_confirmado"],
                                 -SEVERIDADES.index(r["severidade"]),
                                 -(r["ultimo"] or 0)))


ESTILO = """
body{background:#11141a;color:#e6e6e6;font:15px/1.45 system-ui,sans-serif;margin:0;padding:24px}
h1{font-size:20px;margin:0 0 4px}p.sub{color:#8a93a3;margin:0 0 18px}
table{border-collapse:collapse;width:100%}
th,td{padding:8px 10px;text-align:left;border-bottom:1px solid #222831}
th{color:#8a93a3;font-weight:600;font-size:12px;text-transform:uppercase}
tr.confirmado{background:#3a0d12}
tr.confirmado td{color:#ffd9d9;font-weight:600}
td.prefixo{font-family:ui-monospace,monospace}
.sev-alta{color:#ff6b6b}.sev-media{color:#ffcc66}.sev-baixa{color:#8a93a3}
.marca{background:#c0192b;color:#fff;padding:1px 6px;border-radius:3px;font-size:12px}
.vazio{color:#8a93a3;padding:24px 0}
"""


def pagina(estado: dict) -> str:
    linhas = ordenar(estado)
    corpo = "".join(
        "<tr class='{cls}'><td class=prefixo>{pref}</td><td>{marca}</td>"
        "<td class='sev-{sev}'>{sev}</td><td>{sit}</td><td>{col}</td>"
        "<td>{susp}</td><td>{leg}</td></tr>".format(
            cls="confirmado" if r["sequestro_confirmado"] else "",
            pref=html.escape(r["prefixo"]),
            marca=("<span class=marca>SEQUESTRO CONFIRMADO</span>"
                   if r["sequestro_confirmado"]
                   else "<span class=marca>ROTA INSTAVEL</span>"
                   if r["rota_instavel"] else ""),
            sev=r["severidade"],
            sit=html.escape(", ".join(f"{s}×{n}" for s, n in sorted(r["situacoes"].items()))),
            col=len(r["coletores"]),
            susp=r["as_suspeito"] if r["as_suspeito"] is not None else "—",
            leg=r["as_legitimo"] if r["as_legitimo"] is not None else "—")
        for r in linhas)
    tabela = (f"<table><tr><th>prefixo<th>situacao<th>severidade<th>alertas"
              f"<th>coletores<th>AS suspeito<th>AS legitimo</tr>{corpo}</table>"
              if linhas else "<p class=vazio>esperando alertas em bgp.alertas…</p>")
    return (f"<!doctype html><meta charset=utf-8>"
            f"<meta http-equiv=refresh content=2>"
            f"<title>Monitoramento BGP</title><style>{ESTILO}</style>"
            f"<h1>Monitoramento BGP</h1>"
            f"<p class=sub>{len(linhas)} prefixos · bgp.alertas + bgp.derivados</p>"
            f"{tabela}")


class Manipulador(BaseHTTPRequestHandler):
    def do_GET(self):
        with TRAVA:
            copia = copy.deepcopy(ESTADO)  # solta a trava antes de renderizar
        if self.path.startswith("/dados"):
            corpo, tipo = json.dumps(copia, ensure_ascii=False), "application/json"
        elif self.path == "/":
            corpo, tipo = pagina(copia), "text/html"
        else:
            self.send_error(404)
            return
        dados = corpo.encode()
        self.send_response(200)
        self.send_header("Content-Type", f"{tipo}; charset=utf-8")
        self.send_header("Content-Length", str(len(dados)))
        self.end_headers()
        self.wfile.write(dados)

    def log_message(self, *args):
        pass  # o stderr e do consumidor de acoes


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--porta", type=int, default=PORTA)
    p.add_argument("--do-inicio", action="store_true",
                   help="grupo novo comeca no inicio dos topicos, e nao no fim")
    p.add_argument("--grupo", default=GRUPO)
    p.add_argument("--broker", default=BROKER)
    args = p.parse_args(argv)

    from confluent_kafka import Consumer

    consumidor = Consumer({
        "bootstrap.servers": args.broker,
        "group.id": args.grupo,
        "auto.offset.reset": "earliest" if args.do_inicio else "latest",
    })
    consumidor.subscribe(TOPICOS)
    parar = threading.Event()
    thread = threading.Thread(target=consumir, args=(consumidor, parar), daemon=True)
    thread.start()

    servidor = ThreadingHTTPServer(("", args.porta), Manipulador)
    print(f"painel em http://localhost:{args.porta} — {' e '.join(TOPICOS)}",
          file=sys.stderr, flush=True)
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        parar.set()
        thread.join(3)
        consumidor.close()
        servidor.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
