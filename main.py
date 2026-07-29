#!/usr/bin/env python3
"""
Copiador de RabbitMQs
---------------------
Copia N mensagens de cada fila de um RabbitMQ de origem para um
RabbitMQ de destino, sem remover as mensagens originais da origem
(por padrão elas são republicadas de volta após a leitura).

Configuração via arquivo .env (veja .env.example).

Uso:
    python3 main.py
"""

import os
import sys

import pika
import requests
from dotenv import load_dotenv

load_dotenv()


# ------------------------------------------------------------------
# Configuração
# ------------------------------------------------------------------
def env_bool(nome, padrao=False):
    valor = os.getenv(nome)
    if valor is None:
        return padrao
    return valor.strip().lower() in ("1", "true", "yes", "sim")


CONFIG = {
    "origem": {
        "host": os.getenv("ORIGEM_HOST", "localhost"),
        "port": int(os.getenv("ORIGEM_PORT", "5672")),
        "vhost": os.getenv("ORIGEM_VHOST", "/"),
        "user": os.getenv("ORIGEM_USER", "guest"),
        "senha": os.getenv("ORIGEM_PASS", "guest"),
        "ssl": env_bool("ORIGEM_USE_SSL", False),
    },
    "destino": {
        "host": os.getenv("DESTINO_HOST", "localhost"),
        "port": int(os.getenv("DESTINO_PORT", "5672")),
        "vhost": os.getenv("DESTINO_VHOST", "/"),
        "user": os.getenv("DESTINO_USER", "guest"),
        "senha": os.getenv("DESTINO_PASS", "guest"),
        "ssl": env_bool("DESTINO_USE_SSL", False),
    },
    "filas": [f.strip() for f in os.getenv("FILAS", "").split(",") if f.strip()],
    "mensagens_por_fila": int(os.getenv("MENSAGENS_POR_FILA", "5")),
    "preservar_origem": env_bool("PRESERVAR_ORIGEM", True),
}


# ------------------------------------------------------------------
# Conexão RabbitMQ
# ------------------------------------------------------------------
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
        heartbeat=30,
    )
    return pika.BlockingConnection(parametros)


# ------------------------------------------------------------------
# Descoberta de filas via API de gerenciamento
# ------------------------------------------------------------------
def listar_filas_via_api(cfg_api, cfg_origem):
    esquema = "https" if cfg_api["ssl"] else "http"
    vhost = cfg_origem["vhost"]
    vhost_codificado = "%2F" if vhost == "/" else vhost
    url = f"{esquema}://{cfg_api['host']}:{cfg_api['port']}/api/queues/{vhost_codificado}"

    resposta = requests.get(
        url, auth=(cfg_origem["user"], cfg_origem["senha"]), timeout=10
    )
    resposta.raise_for_status()

    return [fila["name"] for fila in resposta.json()]


# ------------------------------------------------------------------
# Cópia de mensagens
# ------------------------------------------------------------------
def copiar_fila(canal_origem, canal_destino, fila, limite, preservar_origem):
    copiadas = 0

    for _ in range(limite):
        metodo, propriedades, corpo = canal_origem.basic_get(
            queue=fila, auto_ack=False
        )

        if metodo is None:
            break  # fila sem mais mensagens

        canal_destino.basic_publish(
            exchange="",
            routing_key=fila,
            body=corpo,
            properties=propriedades,
        )

        if preservar_origem:
            canal_origem.basic_publish(
                exchange="",
                routing_key=fila,
                body=corpo,
                properties=propriedades,
            )

        canal_origem.basic_ack(delivery_tag=metodo.delivery_tag)
        copiadas += 1

    return copiadas


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    filas = CONFIG["filas"]

    if not filas:
        print("Nenhuma fila encontrada na origem.", file=sys.stderr)
        sys.exit(1)

    print(f"Filas a processar ({len(filas)}): {', '.join(filas)}")

    print("Conectando na origem...")
    conexao_origem = conectar(CONFIG["origem"])
    canal_origem = conexao_origem.channel()

    print("Conectando no destino...")
    conexao_destino = conectar(CONFIG["destino"])
    canal_destino = conexao_destino.channel()

    total_geral = 0
    limite = CONFIG["mensagens_por_fila"]
    preservar_origem = CONFIG["preservar_origem"]

    for fila in filas:
        try:
            copiadas = copiar_fila(
                canal_origem, canal_destino, fila, limite, preservar_origem
            )
            print(f"{fila}: {copiadas} mensagem(ns) copiada(s)")
            total_geral += copiadas
        except pika.exceptions.ChannelClosedByBroker as e:
            print(f"{fila}: ERRO ao acessar fila - {e}", file=sys.stderr)
            # o erro fecha o canal; reabre para continuar com as próximas filas
            canal_origem = conexao_origem.channel()
            canal_destino = conexao_destino.channel()

    print(f"\nTotal copiado: {total_geral} mensagens")

    conexao_origem.close()
    conexao_destino.close()


if __name__ == "__main__":
    main()
