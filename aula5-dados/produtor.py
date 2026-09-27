#!/usr/bin/env python3
"""
Produtor da Aula 5: publica as avaliações do avaliacoes.csv no tópico
avaliacoes do Event Hubs, pelo protocolo Kafka, uma por segundo.

Cada avaliação vai com o id do produto como chave. O Kafka escolhe a
partição a partir da chave, então todas as avaliações de um mesmo produto
caem na mesma partição, na ordem em que foram enviadas.

Uso:
    python3 produtor.py                  # o CSV inteiro, uma vez, e termina
    python3 produtor.py --quantidade 20  # só 20 avaliações, e termina
"""

import argparse
import csv
import itertools
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import KafkaError, KafkaException, Producer

TOPICO = "avaliacoes"

PASTA = Path(__file__).resolve().parent

# O librdkafka, a biblioteca em C por baixo do confluent-kafka, escreve
# muito log técnico no terminal. Este logger engole tudo: os erros que
# importam chegam pelo error_cb e viram mensagens em português.
LOG_MUDO = logging.getLogger("librdkafka")
LOG_MUDO.addHandler(logging.NullHandler())
LOG_MUDO.propagate = False


# ---------------------------------------------------------------------------
# Configuração: igual no produtor e no consumidor
# ---------------------------------------------------------------------------

def sair(mensagem, detalhe=None):
    """Mostra o que corrigir e termina com código 1, sem traceback."""
    print(mensagem, file=sys.stderr)
    if detalhe:
        print(f"  ({detalhe})", file=sys.stderr)
    sys.exit(1)


def ler_arquivo_config(caminho):
    """Lê linhas CHAVE=valor. As connection strings têm = e ; no meio."""
    valores = {}
    for linha in caminho.read_text(encoding="utf-8-sig").splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, valor = linha.split("=", 1)
        valor = valor.strip()
        if len(valor) >= 2 and valor[0] == valor[-1] and valor[0] in "'\"":
            valor = valor[1:-1]
        valores[chave.strip()] = valor
    return valores


def carregar_config(chaves):
    """Lê o config.env da pasta do script. O ambiente tem prioridade."""
    arquivo = PASTA / "config.env"
    if not arquivo.exists() and not all(c in os.environ for c in chaves):
        sair("Não encontrei o config.env nesta pasta. "
             "Rode: cp config.env.exemplo config.env && code config.env")
    valores = ler_arquivo_config(arquivo) if arquivo.exists() else {}

    cfg = {}
    for chave in chaves:
        valor = os.environ.get(chave, valores.get(chave, ""))
        # Vazio ou ainda com o valor do config.env.exemplo
        if (not valor or "SUADUPLA" in valor or valor.endswith("...")
                or "0.0.0.0" in valor):
            sair(f"Preencha {chave} no config.env.")
        cfg[chave] = valor

    ns = cfg.get("EVENTHUB_NAMESPACE")
    if ns is not None and ("." in ns or "/" in ns):
        sair("EVENTHUB_NAMESPACE está errado no config.env. "
             "É só o nome do namespace, sem .servicebus.windows.net.")
    conexao = cfg.get("EVENTHUB_CONNECTION")
    if conexao is not None and not conexao.startswith("Endpoint=sb://"):
        sair("EVENTHUB_CONNECTION está errada no config.env. Copie de novo "
             "a connection string inteira, começando em Endpoint=sb://.")
    storage = cfg.get("STORAGE_CONNECTION")
    if storage is not None and not ("AccountName=" in storage
                                    and "AccountKey=" in storage):
        sair("STORAGE_CONNECTION está errada no config.env. Ela precisa ter "
             "AccountName= e AccountKey=. Copie de novo em Access keys, "
             "a da key1, inteira.")
    api = cfg.get("API_URL")
    if api is not None:
        if not api.startswith(("http://", "https://")):
            sair("API_URL está errada no config.env. "
                 "Ela começa com http://, por exemplo http://20.201.10.20:8000.")
        cfg["API_URL"] = api.rstrip("/")
    return cfg


def config_kafka(cfg):
    """A configuração do cliente Kafka para o endpoint Kafka do Event Hubs."""
    return {
        # Este bloco é a única parte do código que sabe que o destino é o
        # Azure. O mesmo código falaria com o Amazon MSK ou com um Kafka
        # próprio trocando só estas linhas.
        "bootstrap.servers": f"{cfg['EVENTHUB_NAMESPACE']}.servicebus.windows.net:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",
        "sasl.password": cfg["EVENTHUB_CONNECTION"],
        # Ajustes que a Microsoft recomenda para clientes librdkafka: o Azure
        # fecha conexões paradas há 240 segundos, então o cliente renova
        # antes disso. Sem compressão: o tier Standard não aceita.
        "socket.keepalive.enable": True,
        "metadata.max.age.ms": 180000,
        "connections.max.idle.ms": 180000,
        # O event hub é criado no portal, nunca por engano pelo script.
        "allow.auto.create.topics": False,
    }


def explicar_falha(falhas, servidor):
    """Traduz os erros do librdkafka nas linhas da tabela do roteiro."""
    def primeira(condicao):
        return next((f for f in falhas if condicao(f)), None)

    # O tier Basic recusa o Kafka na autenticação, então vem antes do SASL
    basic = primeira(lambda f: "Kafka protocol is supported" in f.str())
    if basic:
        sair("O namespace é do tier Basic, que não tem o endpoint Kafka. "
             "Apague o namespace e crie de novo como Standard (Etapa 3 do "
             "roteiro).", basic.str())
    sasl = primeira(lambda f: f.code() == KafkaError._AUTHENTICATION
                    or "SASL authentication" in f.str())
    if sasl:
        sair("SASL authentication error: o Event Hubs recusou a connection "
             "string. Copie de novo EVENTHUB_CONNECTION, inteira, começando "
             "em Endpoint=sb://.", sasl.str())
    nome = primeira(lambda f: f.code() == KafkaError._RESOLVE
                    or "Failed to resolve" in f.str())
    if nome:
        sair(f"Não encontrei o namespace {servidor}. "
             "Confira EVENTHUB_NAMESPACE no config.env.", nome.str())
    sair("O namespace não respondeu na porta 9093. Confira se o pricing tier "
         "é Standard: o Basic não tem Kafka.", falhas[0].str())


def testar_conexao(cliente, cfg, erros):
    """Pede os metadados do tópico: prova a rede, a chave e o tópico de uma vez."""
    servidor = f"{cfg['EVENTHUB_NAMESPACE']}.servicebus.windows.net"
    try:
        metadados = cliente.list_topics(TOPICO, timeout=15)
    except KafkaException as e:
        cliente.poll(0)  # entrega ao error_cb os erros guardados pelo librdkafka
        explicar_falha(erros + [e.args[0]], servidor)
    topico = metadados.topics.get(TOPICO)
    if topico is None or topico.error is not None:
        sair(f"Tópico {TOPICO} não encontrado no namespace "
             f"{cfg['EVENTHUB_NAMESPACE']}. Crie o event hub {TOPICO} com 4 "
             "partições (Etapa 3 do roteiro).")
    return f"{servidor}:9093"


# ---------------------------------------------------------------------------
# Produtor
# ---------------------------------------------------------------------------

def ler_argumentos():
    parser = argparse.ArgumentParser(
        description="Publica as avaliações do avaliacoes.csv no tópico "
                    f"{TOPICO} do Event Hubs, uma por segundo, com o id do "
                    "produto como chave.")
    parser.add_argument(
        "--quantidade", type=inteiro_positivo, metavar="N",
        help="publica N avaliações e termina. Sem a opção, publica o CSV "
             "inteiro uma vez. Se N passar do tamanho do CSV, volta ao começo.")
    return parser.parse_args()


def inteiro_positivo(texto):
    try:
        valor = int(texto)
    except ValueError:
        valor = 0
    if valor < 1:
        raise argparse.ArgumentTypeError("precisa ser um inteiro positivo")
    return valor


def ler_avaliacoes():
    with open(PASTA / "avaliacoes.csv", encoding="utf-8", newline="") as f:
        return [(linha["id_produto"], linha["texto"])
                for linha in csv.DictReader(f)]


def agora_utc():
    """Hora atual em ISO 8601, em UTC, terminada em Z."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def ao_entregar(id_produto, texto, contagem):
    """Cria o callback que o Kafka chama quando confirma (ou não) o envio."""
    def callback(erro, msg):
        if erro is not None:
            print(f"falha ao enviar {id_produto}: {erro.str()}")
            return
        contagem[0] += 1
        aspas = f'"{texto}"'
        print(f"{id_produto:<4} {aspas:<36} → partição {msg.partition()} · offset {msg.offset()}")
    return callback


def esperar(produtor, segundos):
    """Espera sem ficar surdo: os callbacks de entrega continuam chegando."""
    fim = time.monotonic() + segundos
    while (resta := fim - time.monotonic()) > 0:
        produtor.poll(resta)


def main():
    args = ler_argumentos()
    cfg = carregar_config(["EVENTHUB_NAMESPACE", "EVENTHUB_CONNECTION"])
    avaliacoes = ler_avaliacoes()
    quantidade = args.quantidade or len(avaliacoes)

    erros = []
    produtor = Producer({
        **config_kafka(cfg),
        # Recomendação do Event Hubs: o padrão do librdkafka (5 s) é curto demais
        "request.timeout.ms": 60000,
        "error_cb": erros.append,
        "logger": LOG_MUDO,
    })
    servidor = testar_conexao(produtor, cfg, erros)
    print(f"conectado a {servidor} · tópico {TOPICO}")

    contagem = [0]  # avaliações confirmadas pelo Event Hubs
    try:
        envios = itertools.islice(itertools.cycle(avaliacoes), quantidade)
        for n, (id_produto, texto) in enumerate(envios):
            if n > 0:
                esperar(produtor, 1)
            evento = {"id_produto": id_produto, "texto": texto, "ts": agora_utc()}
            # A chave decide a partição: mesmo produto, mesma partição, mesma ordem
            produtor.produce(
                TOPICO,
                key=id_produto.encode("utf-8"),
                value=json.dumps(evento, ensure_ascii=False).encode("utf-8"),
                on_delivery=ao_entregar(id_produto, texto, contagem),
            )
            produtor.poll(0)
    except KeyboardInterrupt:
        pass
    # O flush espera a confirmação de tudo o que já saiu, mesmo depois do Ctrl+C
    try:
        produtor.flush(30)
    except KeyboardInterrupt:
        pass
    total = contagem[0]
    print(f"fim: {total} {'avaliação enviada' if total == 1 else 'avaliações enviadas'}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
