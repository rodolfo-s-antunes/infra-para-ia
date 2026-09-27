# Aula 5 — Roteiro da prática: Storage e streaming

Nas aulas 1 a 4 a API de sentimento só respondia quando alguém chamava. Hoje ela passa a reagir a um fluxo contínuo de avaliações de clientes: cada avaliação chega pelo protocolo Kafka ao Event Hubs, é lida por um consumidor, classificada pela API e guardada como um arquivo no Blob Storage. Você monta as peças pelo portal e pelo Cloud Shell e depois põe os dados para correr em duas abas do terminal. Cada etapa indica onde acontece, o comando exato e o que observar na resposta.

| | |
|---|---|
| **Pré-requisitos** | Conta do Azure ativa, as práticas das aulas 1 a 4 feitas e o repositório `infra-para-ia` clonado no Cloud Shell. A prática é em dupla. |
| **Custo** | Cerca de US$ 0,40, quase todo no namespace do Event Hubs, que cobra por hora enquanto existir, mesmo parado. Por isso a faxina do fim é obrigatória. |

**Como ler as etapas:** TERMINAL acontece no Cloud Shell (Bash), dentro do portal, na pasta `aula5-dados`. PORTAL acontece clicando no portal do Azure, em [portal.azure.com](https://portal.azure.com). NAVEGADOR acontece em uma aba nova do navegador, de preferência anônima.

Nomes de menus e botões do portal ficam em inglês, como a interface mostra. Em vários pontos aparece `<apelido>`: é o apelido da dupla, só com letras minúsculas e dígitos, de 3 a 12 caracteres, sem espaço nem hífen, por exemplo `anaejoao`. Este roteiro é autossuficiente: dá para refazer tudo em casa sozinho.

## Mapa da prática

**Portal e Cloud Shell · montar as peças**

1. Preparar o ambiente e subir a API
2. Storage account, SAS e access tier
3. Event Hubs e `config.env`

**Pipeline · pôr os dados para correr**

4. Produtor: avaliações chegando
5. Consumidor lendo ao vivo
6. Consumidor com a API, gravando no Blob

**Depois do pipeline**

- A. [Atividade avaliativa](ATIVIDADE.md)
- F. Faxina obrigatória

Seis etapas em duas metades, a atividade e a faxina. Os dois momentos em destaque são os que a turma vai lembrar: as avaliações aparecendo ao vivo na segunda aba, e os blobs com o sentimento de cada uma surgindo no portal.

---

## Parte 1. Montar as peças

Três etapas que criam tudo o que o pipeline precisa: a API no ar, uma storage account para guardar os resultados e um namespace do Event Hubs para receber os eventos. No fim da Parte 1 os endereços e as chaves dessas peças vão para um arquivo, o `config.env`, que os scripts leem sozinhos.

### Etapa 1. Preparar o ambiente e subir a API · TERMINAL

Ícone **`>_`** na barra do topo do portal → **Bash**. Se o seu Cloud Shell tiver storage anexado, o clone das aulas anteriores ainda está aí. Se abriu em modo efêmero, clone antes com `git clone https://github.com/rodolfo-s-antunes/infra-para-ia.git`.

```bash
cd infra-para-ia && git pull && cd aula5-dados
pip install --user -r requirements.txt
az group create -n aula5-rg -l brazilsouth
az provider register -n Microsoft.EventHub
az provider register -n Microsoft.Storage
az container create -g aula5-rg -n sentiment-api \
  --image ghcr.io/rodolfo-s-antunes/sentiment-api:v3 \
  --os-type Linux --ip-address Public --ports 8000 \
  --cpu 1 --memory 1 --no-wait
```

O `pip install` instala as duas bibliotecas dos scripts: `confluent-kafka`, um cliente Kafka comum, e `azure-storage-blob`, para gravar no Blob Storage. Os dois `az provider register` habilitam na assinatura os serviços que a aula usa. Se já estiverem habilitados, nada acontece. O último comando sobe a mesma API das aulas anteriores, na versão `v3`, direto do GitHub Container Registry, como no Terraform da aula 4.

> **Por que `--no-wait`.** Sem ele o terminal fica preso até o container subir. Com ele a API sobe em segundo plano enquanto você cria o storage e o Event Hubs no portal. Na Etapa 3 ela já está no ar e você só confere.

> **O que há na pasta `aula5-dados`.** `avaliacoes.csv` (as avaliações de clientes, em português, com o id do produto e o texto), `produtor.py` (publica as avaliações no tópico), `consumidor.py` (lê o tópico e, conforme as opções, chama a API e grava no Blob), `requirements.txt`, `config.env.exemplo` (o modelo do arquivo de configuração), `ROTEIRO.md` e `ATIVIDADE.md`.

> **Ruído esperado.** O `pip` pode imprimir avisos amarelos sobre a própria versão ou sobre pastas fora do `PATH`, e os `az provider register` podem avisar que o registro continua em andamento. Nenhum desses avisos impede as próximas etapas.

### Etapa 2. Storage account, SAS e access tier · PORTAL e NAVEGADOR

#### 2a · Criar a storage account

No portal, busque **Storage accounts** → **+ Create**. Na aba **Basics**, preencha:

| Campo | Valor |
|---|---|
| Resource group | `aula5-rg` |
| Storage account name | `st<apelido>aula5`, por exemplo `stanaejoaoaula5` |
| Region | Brazil South |
| Performance | Standard |
| Redundancy | Locally-redundant storage (LRS) |

O resto fica como está. **Review + create** → **Create** → **Go to resource**. Se o nome for recusado por já existir, é porque o nome de uma storage account é global no Azure inteiro: acrescente um dígito no fim.

#### 2b · Container e o primeiro blob

Antes, no GitHub, abra `aula5-dados/avaliacoes.csv` no repositório e use o botão **Download raw file** para salvar o arquivo no seu computador.

1. Na storage account: **Data storage** → **Containers** → **+ Container** → nome `avaliacoes`, nível de acesso privado → **Create**.
2. Abra o container → **Upload** → escolha o `avaliacoes.csv` → **Upload**.
3. Clique no blob e leia a ficha: a URL, o tamanho, o tipo de conteúdo e o access tier **Hot**.

A hierarquia da teoria na tela:

- **Storage account** `st<apelido>aula5`: nome global, redundância LRS, região Brazil South.
- **Container** `avaliacoes`: privado, agrupa os blobs.
- **Blobs**: `avaliacoes.csv` e, na Etapa 6, `resultados/2-7.json` e outros.

Endereço de um blob: `https://st<apelido>aula5.blob.core.windows.net/avaliacoes/avaliacoes.csv`. A barra em `resultados/` não cria uma pasta: é o começo do nome do blob, e o portal desenha pastas a partir desses prefixos. Na AWS a mesma ideia é conta, bucket e objeto: o Azure tem um nível a mais em cima, a storage account.

#### 2c · Privado por padrão

Na ficha do blob, copie a URL e abra em uma aba anônima do navegador. A resposta é um XML de erro:

```
<Error>
  <Code>PublicAccessNotPermitted</Code>
  <Message>Public access is not permitted on this storage account. ...</Message>
</Error>
```

Ninguém lê um blob sem permissão. Storage accounts novas vêm com o acesso anônimo desligado, e é assim que devem ficar: buckets abertos por engano estão por trás de muitos vazamentos de dados.

#### 2d · Uma permissão com prazo: SAS

1. No blob, aba **Generate SAS**. Deixe **Signing method** em **Account key**.
2. **Permissions**: só **Read**. **Expiry**: daqui a uma hora. **Allowed protocols**: **HTTPS only**.
3. **Generate SAS token and URL** → copie a **Blob SAS URL** e abra na aba anônima: o CSV baixa.

```
https://st<apelido>aula5.blob.core.windows.net/avaliacoes/avaliacoes.csv?sp=r&st=...&se=...&spr=https&sv=...&sr=b&sig=...
```

> **Leia a URL.** `sp=r` é a permissão (só leitura), `se` é a hora em que expira e `sig` é a assinatura, calculada com a chave da conta. Mude um caractere e ela deixa de valer. Guarde o link e abra de novo depois da expiração: a resposta passa a ser `AuthenticationFailed`. Na AWS o equivalente é a presigned URL do S3.

#### 2e · Trocar o access tier

No blob, **Change tier** → **Cool** → **Save**. O arquivo continua legível pela SAS. O que muda é o preço: guardar fica mais barato e ler fica mais caro, com permanência mínima de 30 dias. Nos tiers Cold e Archive a conta vai ainda mais longe, e no Archive o blob fica offline até ser reidratado.

#### 2f · Guardar a conexão para a Etapa 3

Na storage account: **Security + networking** → **Access keys** → **Show** ao lado de **key1** → copie a **Connection string** e deixe à mão, por exemplo num bloco de notas. Ela começa com `DefaultEndpointsProtocol=https` e é o que o consumidor vai usar para gravar no container.

### Etapa 3. Event Hubs e config.env · PORTAL e TERMINAL

#### 3a · Criar o namespace

No portal, busque **Event Hubs** → **+ Create**. Na aba **Basics**:

| Campo | Valor |
|---|---|
| Resource group | `aula5-rg` |
| Namespace name | `eh-<apelido>-aula5`, por exemplo `eh-anaejoao-aula5` |
| Location | Brazil South |
| Pricing tier | Standard |
| Throughput units | 1 |

**Review + create** → **Create** → **Go to resource**.

> **Standard, nunca Basic.** O tier Basic é mais barato, mas não tem o endpoint Kafka. Com ele os scripts não conectam e avisam que o namespace é Basic. Se você criou como Basic, apague o namespace e crie de novo como Standard.

#### 3b · Criar o event hub

No namespace: **+ Event Hub**.

| Campo | Valor |
|---|---|
| Name | `avaliacoes` |
| Partition count | 4 |
| Cleanup policy e retention | Delete, 24 horas |

**Review + create** → **Create**. No tier Standard o número de partições não aumenta depois de criado: escolher agora é escolher o teto de paralelismo do consumidor.

Os nomes do Event Hubs traduzidos para o vocabulário do Kafka:

- **Namespace** `eh-<apelido>-aula5` é o **cluster**, com o endpoint Kafka na porta 9093.
- **Event hub** `avaliacoes` é o **tópico**, com retenção de 24 horas.
- **Partições** P0 · P1 · P2 · P3: cada uma um log ordenado, com o próprio offset.

Para um cliente Kafka o namespace é o cluster e o event hub é o tópico. O código dos scripts não sabe que está falando com o Azure.

#### 3c · A connection string do namespace

No namespace: **Settings** → **Shared access policies** → **RootManageSharedAccessKey** → copie a connection string da primary key. Ela começa com `Endpoint=sb://`. É a chave mestra do namespace: serve para a aula, mas em produção cada aplicação recebe uma política própria, só com Send ou só com Listen.

#### 3d · A API respondeu? Então preencha o config.env

```bash
IP=$(az container show -g aula5-rg -n sentiment-api \
   --query ipAddress.ip -o tsv)
curl -s http://$IP:8000/versao
echo "API_URL=http://$IP:8000"
cp config.env.exemplo config.env
code config.env
```

Saída esperada (resumida):

```
{"versao":"v3"}
API_URL=http://20.201.xx.xx:8000
```

Se o `curl` não responder, o container ainda está subindo: espere um pouco e repita. No editor que abre, preencha as quatro linhas com os valores da dupla, cole as duas connection strings inteiras e sem aspas, salve com **Ctrl+S** e feche com **Ctrl+Q**:

```
EVENTHUB_NAMESPACE=eh-anaejoao-aula5
EVENTHUB_CONNECTION=Endpoint=sb://eh-anaejoao-aula5.servicebus.windows.net/;...
STORAGE_CONNECTION=DefaultEndpointsProtocol=https;AccountName=stanaejoaoaula5;...
API_URL=http://20.201.xx.xx:8000
```

> **O que os scripts montam com isso.** Com os dois primeiros valores, `produtor.py` e `consumidor.py` montam a configuração padrão de um cliente Kafka: `bootstrap.servers` igual a `<namespace>.servicebus.windows.net:9093`, `security.protocol` igual a `SASL_SSL`, `sasl.mechanism` igual a `PLAIN`, o usuário literal `$ConnectionString` e a connection string como senha. Esse bloco é a única parte que sabe que o destino é o Azure. O mesmo código falaria com o Amazon MSK ou com um Kafka próprio trocando só essas linhas.

> **O `config.env` guarda duas chaves.** Quem tiver o arquivo escreve no seu tópico e no seu storage. Ele já está no `.gitignore` do repositório: não vai para o Git, e também não deve aparecer nas capturas da atividade. As chaves deixam de valer quando o resource group é apagado.

---

## Parte 2. O pipeline rodando

Três etapas com dois terminais abertos lado a lado. Na aba 1 o produtor publica avaliações no tópico. Na aba 2 o consumidor lê o que chega, primeiro só imprimindo e depois chamando a API e gravando cada resultado no Blob.

### Etapa 4. Produtor: avaliações chegando · TERMINAL · ABA 1

```bash
python3 produtor.py
```

Saída esperada (resumida):

```
conectado a eh-anaejoao-aula5.servicebus.windows.net:9093 · tópico avaliacoes
P07  "chegou rápido e funciona bem"       → partição 2 · offset 0
P03  "a bateria não dura nem meio dia"    → partição 3 · offset 0
P07  "recomendo, ótimo custo-benefício"   → partição 2 · offset 1
P11  "veio com a caixa amassada"          → partição 2 · offset 2
```

O produtor envia uma avaliação por segundo e percorre o CSV inteiro. Cada avaliação vai com o id do produto como **chave**, e o Kafka escolhe a partição a partir da chave: toda avaliação do P07 cai na partição 2, na ordem em que foi feita. Produtos diferentes podem dividir uma partição, como o P11, que também cai na 2. O **offset** conta por partição, começando do zero: a partição 2 já está no offset 2 enquanto a 3 recebe o seu primeiro evento. Não existe um offset do tópico inteiro. Deixe o produtor rodando nesta aba.

> **Confira no portal.** No namespace, **Overview**: o gráfico de mensagens recebidas (**Incoming Messages**) começa a subir alguns instantes depois do produtor. No event hub `avaliacoes` aparecem as quatro partições. Os eventos estão guardados no log e continuam lá depois de lidos, até a retenção de 24 horas vencer.

### Etapa 5. Consumidor lendo ao vivo · TERMINAL · ABA 2 · momento de destaque

Abra uma segunda sessão no Cloud Shell com o botão **New session** da barra do terminal. Na aba nova:

```bash
cd infra-para-ia/aula5-dados
python3 consumidor.py --grupo leitura
```

Saída esperada (resumida):

```
grupo leitura · partições atribuídas: 0, 1, 2, 3
p2 · off 3   P07   chegou rápido, mas a tela veio riscada
p1 · off 0   P02   ótimo custo, uso todo dia
p3 · off 1   P03   parou de carregar em uma semana
```

O consumidor entra no **consumer group** chamado `leitura`. Como é o único membro, o grupo entrega a ele as quatro partições. Um grupo novo começa do fim do log e lê só o que chega depois dele, por isso o produtor precisa estar rodando na aba 1. Faça o teste: **Ctrl+C** e rode o mesmo comando de novo. O consumidor continua exatamente de onde parou, porque o grupo guardou o offset de cada partição.

> **O grupo não aparece no portal.** A aba **Consumer groups** do event hub mostra só o grupo `$Default`. Os grupos do Kafka são criados no primeiro uso, valem para o namespace inteiro e só existem para os clientes Kafka. Não é erro.

O log entre o produtor e o consumidor:

- **Aba 1, produtor**: uma avaliação por segundo, chave igual ao id do produto.
- **Tópico `avaliacoes`**: quatro partições, cada uma com a sua sequência de offsets 0, 1, 2, ...
- **Aba 2, grupo `leitura`**: um consumidor com as quatro partições e um offset guardado por partição.

Cada partição é lida por um único membro do grupo. Ler não apaga. Outro grupo, com outro nome, leria os mesmos eventos de novo, com os próprios offsets.

### Etapa 6. Consumidor com a API, gravando no Blob · TERMINAL e PORTAL · o momento central

Na aba 2, **Ctrl+C** no consumidor anterior e:

```bash
python3 consumidor.py --grupo pipeline --api --blob
```

Saída esperada (resumida):

```
grupo pipeline · partições atribuídas: 0, 1, 2, 3
p1 · off 4   P09   negativo (0,94)   → resultados/1-4.json
p2 · off 7   P11   positivo (0,94)   → resultados/2-7.json
```

Um grupo novo, `pipeline`, com duas opções a mais: `--api` manda cada avaliação para o `POST /prediz` da API, no endereço do `API_URL`, e `--blob` grava a resposta como um arquivo JSON no container `avaliacoes`. Se o produtor já terminou o CSV, rode `python3 produtor.py` de novo na aba 1 e veja o grupo acompanhar.

> **Confira no portal.** Container `avaliacoes` → pasta `resultados`: os blobs surgem enquanto o consumidor roda. Clique em um e use **Edit** para ler o conteúdo. **Cada avaliação que chegou pelo Kafka virou um arquivo com o sentimento que a API decidiu.** É a frase para guardar desta prática.
>
> ```
> {"id_produto": "P11", "texto": "...", "sentimento": "positivo",
>  "confianca": 0.9443, "particao": 2, "offset": 7}
> ```

O nome do blob combina partição e offset, e isso tem motivo. Dois eventos nunca têm o mesmo par, então os nomes nunca colidem. E se o consumidor cair depois de gravar e antes de registrar o offset, o Kafka entrega o mesmo evento de novo, a garantia chamada **at-least-once**: o reprocessamento sobrescreve o mesmo blob, e a duplicata não faz estrago.

O sistema completo do curso, montado pela dupla em uma aula:

1. **Produtor**: avaliações do CSV.
2. **Event Hubs**: tópico `avaliacoes`, 4 partições.
3. **Consumidor**: grupo `pipeline`.
4. **API**: `POST /prediz` num container.
5. **Blob Storage**: um JSON por avaliação.

A API nasceu num container (aula 1), ganhou cluster e réplicas (aulas 2 e 3), virou código (aula 4) e agora reage a eventos e guarda o que decide.

> **Compare com as aulas anteriores.** Até aqui a API era chamada por alguém com `curl` ou pela página `/docs`. Agora quem chama é outro programa, no ritmo em que os eventos chegam. Se a API ficar lenta, nada se perde: as avaliações esperam no log, e o atraso do consumidor, o **lag**, é o sinal de que é hora de mais consumidores. No Kubernetes da aula 3, o KEDA escala réplicas exatamente por esse número.

---

## Parte 3. Custo e faxina

### Faxina obrigatória · TERMINAL

**Não saia da aula sem fazer esta etapa.** Colete antes as capturas da atividade que já tiver feito. O namespace do Event Hubs cobra por hora enquanto existir, mesmo sem nenhum evento passando.

```bash
# Ctrl+C no produtor (aba 1) e no consumidor (aba 2), depois:
az group delete -n aula5-rg --yes --no-wait
az group list -o table
```

Um comando leva tudo: a API, a storage account com os blobs e o namespace com o event hub. O `--no-wait` devolve o terminal na hora, e o grupo some da lista depois de alguns instantes. As chaves do `config.env` deixam de valer: quando recriar as peças para terminar a atividade, copie as novas.

> **Entre a aula e a entrega.** A atividade tem prazo de quatro dias, e as peças não devem ficar ligadas nesse intervalo. Para terminar em casa, refaça as Etapas 1 a 3 com os mesmos nomes, atualize o `config.env` e siga o [`ATIVIDADE.md`](ATIVIDADE.md). Depois da entrega, faça a limpeza geral da conta: nenhum resource group das aulas anteriores deve sobrar.

Em **Gerenciamento de Custos** (Cost Management) → **Análise de custo** (Cost analysis) o consumo aparece com atraso de algumas horas até um dia. O esperado para a aula é algo em torno de US$ 0,40: cerca de US$ 0,25 do namespace Standard com 1 throughput unit por duas horas, cerca de US$ 0,15 do container ACI com 1 vCPU e 1 GB e centavos da storage account.

### Checklist final da prática

- [ ] `aula5-rg` criado e a API disparada com `--no-wait`
- [ ] Storage account `st<apelido>aula5` com o container `avaliacoes`
- [ ] URL sem credencial recusada com `PublicAccessNotPermitted`
- [ ] SAS de leitura aberta no navegador e o blob trocado para Cool
- [ ] Namespace `eh-<apelido>-aula5` no tier Standard
- [ ] Event hub `avaliacoes` com 4 partições
- [ ] `curl` em `/versao` respondendo `v3`
- [ ] `config.env` com as quatro linhas preenchidas
- [ ] Produtor mostrando partição e offset de cada envio
- [ ] Grupo `leitura` recebendo as quatro partições ao vivo
- [ ] Blobs em `resultados/` com o sentimento de cada avaliação
- [ ] Capturas da atividade coletadas
- [ ] `aula5-rg` apagado e fora do `az group list`

---

## Erros comuns

| Sintoma | O que rodar ou olhar | Causa provável e saída |
|---|---|---|
| `ModuleNotFoundError: No module named 'confluent_kafka'` | `pip install --user -r requirements.txt` | A sessão do Cloud Shell reiniciou em modo efêmero e perdeu os pacotes. Instale de novo. |
| `Failed to resolve` com o endereço `servicebus.windows.net` | `EVENTHUB_NAMESPACE` no `config.env` | Nome do namespace digitado errado. É só o nome, sem `.servicebus.windows.net`. |
| `SASL authentication error` | `EVENTHUB_CONNECTION` no `config.env` | Connection string cortada ou com aspas. Copie de novo, inteira, começando em `Endpoint=sb://`. |
| `O namespace é do tier Basic` | Pricing tier do namespace, no **Overview** | O tier Basic não tem Kafka. Apague o namespace e crie de novo como Standard. |
| Tópico `avaliacoes` não encontrado | Lista de event hubs do namespace | O event hub não foi criado ou tem outro nome. O nome precisa ser exatamente `avaliacoes`. |
| Consumidor conecta e não mostra nada | A aba 1 ainda está produzindo? | Um grupo novo lê só o que chega depois dele. Rode `python3 produtor.py` de novo. |
| Erro ao chamar a API | `curl -s http://<IP>:8000/versao` | `API_URL` errada no `config.env`, ou o container ainda está subindo. Espere um pouco e repita. |
| `AuthenticationFailed` ao gravar o blob | `STORAGE_CONNECTION` no `config.env` | Connection string incompleta. Copie de novo em **Access keys**, a da key1, inteira. |
| `AuthenticationFailed` ao abrir a SAS no navegador | A hora de expiração da SAS | A SAS venceu ou a URL foi copiada pela metade. Gere outra. |
| Nome da storage account recusado | Mensagem do campo no portal | O nome já existe em algum lugar do Azure. Acrescente um dígito no fim. |
| `MissingSubscriptionRegistration` | `az provider register -n <namespace do erro>` | Serviço ainda não habilitado na assinatura. Aguarde o estado `Registered` e repita. |
| `RequestDisallowedByAzure` citando regiões | `python3 check_azure.py` na raiz do repositório | A sua assinatura não libera `brazilsouth`. Escolha uma região da lista e use a mesma nas três peças. |

---

## Anexo A. Comandos de referência

### Azure CLI

| Comando | Para quê |
|---|---|
| `az group create -n aula5-rg -l brazilsouth` | Criar o resource group que guarda as três peças da aula. |
| `az provider register -n Microsoft.EventHub` | Habilitar um serviço na assinatura. Sem efeito se já estiver habilitado. |
| `az container create ... --no-wait` | Subir a API no ACI sem prender o terminal. |
| `az container show -g aula5-rg -n sentiment-api --query ipAddress.ip -o tsv` | Descobrir o IP público da API, que vai para o `API_URL`. |
| `az storage blob list --account-name <storage> -c avaliacoes --prefix resultados/ -o table` | Listar os blobs de resultado pelo terminal, sem abrir o portal. |
| `az group delete -n aula5-rg --yes --no-wait` | Apagar todas as peças da aula de uma vez. |
| `az group list -o table` | Conferir a faxina. |

### Scripts da aula

| Comando | Para quê |
|---|---|
| `python3 produtor.py` | Publicar as avaliações do CSV no tópico, uma por segundo, com o id do produto como chave. |
| `python3 produtor.py --quantidade 20` | Publicar só 20 avaliações e parar. Usado na atividade. |
| `python3 consumidor.py --grupo NOME` | Ler o tópico como membro do grupo `NOME`, só imprimindo. |
| `--api` | Chamar o `POST /prediz` da API para cada avaliação. |
| `--blob` | Gravar cada resultado como `<prefixo>/<partição>-<offset>.json` no container `avaliacoes`. |
| `--prefixo NOME` | Trocar o começo do nome dos blobs. O padrão é `resultados`. |
| `--desde-o-inicio` | Para um grupo novo, começar do offset 0 em vez do fim do log. |

---

## Anexo B. Glossário

| Termo | Em uma frase |
|---|---|
| Object storage | Armazenamento de arquivos inteiros acessados por HTTP, cada um com uma chave. No Azure, Blob Storage. Na AWS, S3. |
| Storage account | O nível de cima do Blob Storage, com nome global, redundância e rede. Agrupa os containers. |
| Container | No Blob Storage, o recipiente que agrupa blobs e permissões. Equivale ao bucket do S3. Não confundir com o container da aula 1. |
| Blob | Um arquivo guardado, com bytes, metadados e um endereço HTTP próprio. |
| Access tier | A faixa de preço de um blob: hot, cool, cold ou archive. Quanto mais barato guardar, mais caro ler. |
| SAS | Shared Access Signature: uma URL assinada com a permissão e o prazo embutidos. |
| LRS | Três cópias de cada dado dentro de um mesmo datacenter. A redundância mais barata. |
| Evento | Um fato que já aconteceu, como uma avaliação feita. Não se edita: uma mudança é outro evento. |
| Log distribuído | Uma sequência de eventos gravada em ordem e guardada por um tempo, que vários leitores leem no próprio ritmo. |
| Tópico | O nome de um fluxo de eventos. No Event Hubs, o event hub. |
| Partição | Um pedaço independente e ordenado do tópico. O número de partições é o teto de paralelismo de um grupo. |
| Chave | O valor que decide a partição de um evento. Mesma chave, mesma partição, mesma ordem. |
| Offset | A posição de um evento dentro da partição, contada a partir do zero. |
| Consumer group | Consumidores que dividem o trabalho. Cada partição vai para um único membro, e o grupo guarda até onde leu. |
| Rebalanceamento | A redistribuição das partições quando um consumidor entra ou sai do grupo. |
| Retenção | Por quanto tempo o log guarda os eventos, lidos ou não. Hoje, 24 horas. |
| Lag | A distância entre o último evento do log e o offset do grupo: o quanto o consumidor está atrasado. |
| Replay | Reler o histórico do log com um grupo novo, por exemplo para reprocessar tudo com um modelo novo. |
| At-least-once | A garantia de que todo evento é entregue pelo menos uma vez. Duplicatas podem acontecer, e o consumidor precisa tolerá-las. |
| Namespace | No Event Hubs, o contêiner dos event hubs, com o endpoint e a cobrança. Para o Kafka, o cluster. |
| Throughput unit | A unidade de capacidade e de cobrança do Event Hubs Standard. Hoje, uma. |

---

## Atividade avaliativa

Com o pipeline no ar, a atividade da semana põe as peças à prova: uma mudança no `consumidor.py` e quatro experimentos, cada um ligado a um conceito da teoria. A entrega é em dupla, com prazo de 4 dias, e as instruções completas estão em [`ATIVIDADE.md`](ATIVIDADE.md).
