#!/usr/bin/env python3
"""
Purge de filas - Copiador de RabbitMQs
---------------------------------------
Esvazia (purge) as filas na base de DESTINO configurada no .env.

Usa as mesmas variáveis do main.py:
- DESTINO_* para a conexão

Uso:
    python3 purge.py            # pede confirmação antes de purgar
    python3 purge.py --sim      # purga sem pedir confirmação
"""

import os
import sys

import pika
import requests
from dotenv import load_dotenv

load_dotenv()


def env_bool(nome, padrao=False):
    valor = os.getenv(nome)
    if valor is None:
        return padrao
    return valor.strip().lower() in ("1", "true", "yes", "sim")


CONFIG = {
    "destino": {
        "host": os.getenv("DESTINO_HOST", "localhost"),
        "port": int(os.getenv("DESTINO_PORT", "5672")),
        "vhost": os.getenv("DESTINO_VHOST", "/"),
        "user": os.getenv("DESTINO_USER", "guest"),
        "senha": os.getenv("DESTINO_PASS", "guest"),
        "ssl": env_bool("DESTINO_USE_SSL", False),
    },
    # API de gerenciamento do DESTINO, usada só se FILAS estiver vazio.
    # Se você usa a mesma porta de API em ambos, pode reaproveitar
    # DESTINO_HOST; caso contrário, defina DESTINO_API_HOST/PORT no .env.
    "api_destino": {
        "host": os.getenv("DESTINO_API_HOST", os.getenv("DESTINO_HOST", "localhost")),
        "port": int(os.getenv("DESTINO_API_PORT", "15672")),
        "ssl": env_bool("DESTINO_API_SSL", False),
    },
    "filas": [f.strip() for f in os.getenv("FILAS", "").split(",") if f.strip()],
}


def conectar(cfg):
    credenciais = pika.PlainCredentials(cfg["user"], cfg["senha"])
    parametros = pika.ConnectionParameters(
        host=cfg["host"],
        port=cfg["port"],
        virtual_host=cfg["vhost"],
        credentials=credenciais,
        ssl_options=pika.SSLOptions(context=__import__("ssl").create_default_context())
        if cfg["ssl"]
        else None,
        heartbeat=120,
        blocked_connection_timeout=60
    )
    return pika.BlockingConnection(parametros)


def main():
    confirmar_automaticamente = "--sim" in sys.argv

    filas = CONFIG["filas"]

    if not filas:
        print("Nenhuma fila encontrada no vhost de destino.")
        sys.exit(0)

    destino = CONFIG["destino"]
    print(f"Destino: {destino['host']}:{destino['port']} vhost={destino['vhost']}")
    print(f"Filas que serão ESVAZIADAS ({len(filas)}): {', '.join(filas)}")

    if not confirmar_automaticamente:
        resposta = input("\nConfirma o purge dessas filas? Digite 'sim' para continuar: ")
        if resposta.strip().lower() != "sim":
            print("Cancelado.")
            sys.exit(0)

    conexao = conectar(destino)
    canal = conexao.channel()

    total_removido = 0

    for fila in filas:
        try:
            resultado = canal.queue_purge(queue=fila)
            removidas = resultado.method.message_count
            print(f"{fila}: {removidas} mensagem(ns) removida(s)")
            total_removido += removidas
        except pika.exceptions.ChannelClosedByBroker as e:
            print(f"{fila}: ERRO ao purgar - {e}", file=sys.stderr)
            canal = conexao.channel()

    print(f"\nTotal removido: {total_removido} mensagens")
    conexao.close()


if __name__ == "__main__":
    main()