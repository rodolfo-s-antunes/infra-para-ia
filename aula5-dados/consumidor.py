#!/usr/bin/env python3
"""
Consumidor da Aula 5: lê o tópico avaliacoes do Event Hubs como membro de
um consumer group, pelo protocolo Kafka.

    sem opções      só imprime cada avaliação
    --api           chama POST /prediz da API de sentimento para cada uma
    --api --blob    grava cada resultado como um JSON no container avaliacoes

O grupo guarda até onde leu (o offset de cada partição). Parar e rodar de
novo com o mesmo --grupo continua de onde parou. Um grupo com outro nome lê
os mesmos eventos de novo, com os próprios offsets.

Uso:
    python3 consumidor.py --grupo leitura
    python3 consumidor.py --grupo pipeline --api --blob
    python3 consumidor.py --grupo replay1 --desde-o-inicio --api --blob --prefixo reprocessado
"""

import argparse
import http.client
import json
import logging
import os
import sys
import time
import urllib.request
from pathlib import Path

from azure.core.exceptions import ClientAuthenticationError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings
from confluent_kafka import Consumer, KafkaError, KafkaException

TOPICO = "avaliacoes"
CONTAINER = "avaliacoes"

PASTA = Path(__file__).resolve().parent

# O librdkafka, a biblioteca em C por baixo do confluent-kafka, escreve
# muito log técnico no terminal. Este logger engole tudo: os erros que
# importam chegam pelo error_cb e viram mensagens em português.
LOG_MUDO = logging.getLogger("librdkafka")
LOG_MUDO.addHandler(logging.NullHandler())
LOG_MUDO.propagate = False

ESPERAS_API = (1, 2, 4)  # segundos antes de cada nova tentativa na API


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
# API de sentimento e Blob Storage
# ---------------------------------------------------------------------------

def testar_api(api_url):
    try:
        with urllib.request.urlopen(f"{api_url}/versao", timeout=10) as resposta:
            json.load(resposta)
    except (OSError, ValueError, http.client.HTTPException):
        sair(f"Não consegui falar com a API em {api_url}. Confira API_URL no "
             "config.env ou espere o container terminar de subir.")


def chamar_api(api_url, texto):
    pedido = urllib.request.Request(
        f"{api_url}/prediz",
        data=json.dumps({"texto": texto}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(pedido, timeout=10) as resposta:
        dados = json.load(resposta)
    return dados["sentimento"], float(dados["confianca"])


def classificar(api_url, texto):
    """Chama a API e, se falhar, tenta de novo com esperas crescentes."""
    for espera in ESPERAS_API + (None,):
        try:
            return chamar_api(api_url, texto)
        except (OSError, ValueError, KeyError, http.client.HTTPException):
            if espera is None:
                return None
            time.sleep(espera)


def abrir_container(conexao):
    """Confere, antes de ler o tópico, que o container existe e a chave vale."""
    try:
        container = BlobServiceClient.from_connection_string(
            conexao).get_container_client(CONTAINER)
        # Poucas tentativas aqui: um nome errado deve aparecer logo, não em um minuto
        container.get_container_properties(retry_total=1)
    except ResourceNotFoundError:
        sair(f"Não encontrei o container {CONTAINER} na storage account. "
             "Crie o container na Etapa 2 do roteiro.")
    except (ClientAuthenticationError, ValueError) as e:
        sair("AuthenticationFailed: a storage account recusou a connection "
             "string. Copie de novo STORAGE_CONNECTION em Access keys, a da "
             "key1, inteira.", str(e).splitlines()[0])
    except Exception as e:
        sair("Não consegui falar com a storage account. Confira "
             "STORAGE_CONNECTION no config.env.", str(e).splitlines()[0])
    return container


def gravar_blob(container, nome_blob, resultado):
    container.upload_blob(
        nome_blob,
        json.dumps(resultado, ensure_ascii=False, indent=2).encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )


# ---------------------------------------------------------------------------
# Consumidor
# ---------------------------------------------------------------------------

def ler_argumentos():
    parser = argparse.ArgumentParser(
        description=f"Lê o tópico {TOPICO} do Event Hubs como membro de um "
                    "consumer group. Sem opções, só imprime cada avaliação.")
    parser.add_argument(
        "--grupo", required=True, metavar="NOME",
        help="nome do consumer group (o group.id do Kafka). Obrigatória.")
    parser.add_argument(
        "--api", action="store_true",
        help="chama POST /prediz da API (API_URL no config.env) para cada avaliação")
    parser.add_argument(
        "--blob", action="store_true",
        help=f"grava cada resultado como um JSON no container {CONTAINER}. Exige --api.")
    parser.add_argument(
        "--prefixo", default="resultados", metavar="NOME",
        help="começo do nome dos blobs (padrão: resultados)")
    parser.add_argument(
        "--desde-o-inicio", action="store_true",
        help="para um grupo que ainda não tem offset guardado, começa do "
             "offset 0 em vez do fim do log")
    return parser.parse_args()


def ao_atribuir(grupo, desde_o_inicio):
    def callback(consumidor, particoes):
        if particoes:
            lista = ", ".join(str(p) for p in sorted(p.partition for p in particoes))
            print(f"grupo {grupo} · partições atribuídas: {lista}")
            anotar_ponto_de_partida(consumidor, particoes, desde_o_inicio)
        else:
            print(f"grupo {grupo} · nenhuma partição atribuída: consumidor ocioso")
    return callback


def anotar_ponto_de_partida(consumidor, particoes, desde_o_inicio):
    """
    O Kafka só guarda o offset de uma partição depois do primeiro commit nela.
    Uma partição que ainda não recebeu nada ficaria sem offset, e o que
    chegasse nela com o consumidor parado seria pulado na volta. Por isso,
    na primeira vez, o grupo já anota de onde começa cada partição.
    """
    try:
        guardados = consumidor.committed(particoes, timeout=10)
        novas = [p for p in guardados if p.offset < 0]
        for p in novas:
            inicio, fim = consumidor.get_watermark_offsets(p, timeout=10)
            p.offset = inicio if desde_o_inicio else fim
        if novas:
            consumidor.commit(offsets=novas, asynchronous=False)
    except KafkaException:
        pass  # sem a anotação, vale o auto.offset.reset, como em qualquer cliente Kafka


def ao_revogar(grupo):
    def callback(consumidor, particoes):
        print(f"grupo {grupo} · rebalanceamento: partições devolvidas ao grupo")
    return callback


def confirmar(consumidor, msg):
    """Grava no grupo o offset da mensagem: a partir daqui ela não volta mais."""
    try:
        consumidor.commit(message=msg, asynchronous=False)
    except KafkaException as e:
        # Acontece num rebalanceamento: o novo dono da partição relê o evento
        print(f"p{msg.partition()} · off {msg.offset()}   offset não confirmado, "
              f"o evento pode ser reprocessado ({e.args[0].str()})")


def encerrar_sem_confirmar(consumidor, motivo):
    """Sai sem commit: o evento continua no log e será entregue de novo."""
    print(f"{motivo}. Encerrando sem confirmar o offset: rode de novo e o "
          "evento será reprocessado.", file=sys.stderr)
    consumidor.close()
    sys.exit(1)


def processar(msg, args, cfg, consumidor, container):
    onde = f"p{msg.partition()} · off {msg.offset()}"
    try:
        evento = json.loads(msg.value())
        id_produto, texto = evento["id_produto"], evento["texto"]
    except (TypeError, ValueError):
        print(f"{onde}   mensagem ignorada: não é JSON")
        confirmar(consumidor, msg)
        return
    except KeyError:
        print(f"{onde}   mensagem ignorada: faltam id_produto e texto")
        confirmar(consumidor, msg)
        return

    if not args.api:
        print(f"{onde}   {id_produto}   {texto}")
        confirmar(consumidor, msg)
        return

    resposta = classificar(cfg["API_URL"], texto)
    if resposta is None:
        encerrar_sem_confirmar(
            consumidor, f"a API não respondeu para p{msg.partition()} · off {msg.offset()}")
    sentimento, confianca = resposta
    classe = f"{sentimento} ({confianca:.2f})".replace(".", ",")

    if not args.blob:
        print(f"{onde}   {id_produto}   {classe}")
        confirmar(consumidor, msg)
        return

    # Nome do blob: prefixo/partição-offset.json. A atividade (E1) pede para trocar o prefixo pelo sentimento.
    nome_blob = f"{args.prefixo}/{msg.partition()}-{msg.offset()}.json"
    resultado = {
        "id_produto": id_produto,
        "texto": texto,
        "sentimento": sentimento,
        "confianca": confianca,
        "particao": msg.partition(),
        "offset": msg.offset(),
    }
    try:
        gravar_blob(container, nome_blob, resultado)
    except Exception as e:
        encerrar_sem_confirmar(
            consumidor, f"não consegui gravar {nome_blob} ({str(e).splitlines()[0]})")
    # Primeiro grava, depois confirma. Se o processo cair entre as duas
    # linhas, o evento volta e sobrescreve o mesmo blob: at-least-once.
    confirmar(consumidor, msg)
    print(f"{onde}   {id_produto}   {classe}   → {nome_blob}")


def main():
    args = ler_argumentos()
    if args.blob and not args.api:
        sair("--blob precisa de --api: o blob guarda o sentimento que a API devolve.")

    chaves = ["EVENTHUB_NAMESPACE", "EVENTHUB_CONNECTION"]
    if args.api:
        chaves.append("API_URL")
    if args.blob:
        chaves.append("STORAGE_CONNECTION")
    cfg = carregar_config(chaves)

    erros = []
    consumidor = Consumer({
        **config_kafka(cfg),
        "group.id": args.grupo,
        # O commit é manual, feito só depois que a avaliação foi processada.
        # Com o commit automático, um evento podia ser dado como lido e se perder.
        "enable.auto.commit": False,
        # Vale só para um grupo sem offset guardado: latest é o fim do log,
        # earliest é o offset 0. Um grupo conhecido continua de onde parou.
        "auto.offset.reset": "earliest" if args.desde_o_inicio else "latest",
        # Recomendações do Event Hubs para consumidores librdkafka
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 3000,
        "error_cb": erros.append,
        "logger": LOG_MUDO,
    })
    testar_conexao(consumidor, cfg, erros)
    if args.api:
        testar_api(cfg["API_URL"])
    container = abrir_container(cfg["STORAGE_CONNECTION"]) if args.blob else None

    consumidor.subscribe([TOPICO], on_assign=ao_atribuir(args.grupo, args.desde_o_inicio),
                         on_revoke=ao_revogar(args.grupo))
    try:
        while True:
            msg = consumidor.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"aviso do Kafka: {msg.error().str()}")
                continue
            processar(msg, args, cfg, consumidor, container)
    except KeyboardInterrupt:
        pass
    # O close avisa o grupo que este membro saiu. Os outros membros recebem
    # as partições na hora, sem esperar o session.timeout.ms vencer.
    consumidor.close()
    print(f"consumidor encerrado · grupo {args.grupo}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
