# Aula 5 — Atividade avaliativa da semana

| | |
|---|---|
| **Entrega** | Em dupla, prazo de 4 dias, um único PDF no ambiente da disciplina, com as cinco evidências abaixo. |
| **Missão** | Provar que o pipeline aguenta o que acontece em produção: uma mudança de código e quatro experimentos, cada um ligado a um conceito da teoria. |

Na prática o pipeline funcionou uma vez, com tudo no lugar. Nesta atividade vocês mexem nele como acontece no dia a dia: mudam o código do consumidor, põem dois consumidores para dividir o trabalho, derrubam o consumidor no meio do fluxo e reprocessam o histórico inteiro.

## Antes de começar

As peças das Etapas 1 a 3 do [roteiro](ROTEIRO.md) precisam estar no ar: a API no ACI, a storage account com o container `avaliacoes` e o namespace do Event Hubs com o event hub `avaliacoes` de 4 partições.

O namespace Standard cobra por hora mesmo parado. Por isso, apaguem o `aula5-rg` ao sair da aula e recriem pelas Etapas 1 a 3 quando forem terminar a atividade, **com os mesmos nomes**. Ao recriar, atualizem o `config.env` com os valores novos: as duas connection strings mudam, e o IP da API também, então o `API_URL` muda junto.

```bash
cd infra-para-ia && git pull && cd aula5-dados
code config.env
```

Um event hub recém-criado está vazio. Antes das evidências, deixem o `python3 produtor.py` percorrer o CSV pelo menos uma vez, com o consumidor do grupo `pipeline` rodando na outra aba, como na Etapa 6. Assim o log tem histórico para a E4 e o grupo `pipeline` já tem offset guardado para a E3.

## Ordem sugerida

A mudança da E1 troca o prefixo do nome dos blobs pelo sentimento. Depois dela, a opção `--prefixo` deixa de fazer efeito, e a E4 precisa dela. Façam nesta ordem, com o `consumidor.py` original: **E2, E3, E4**, e só então a **E1**. A **E5** é sempre a última.

Se já alteraram o código, voltem a linha original com `git checkout consumidor.py` antes da E4 e refaçam a alteração depois.

## Evidências

### E1 · Um blob por sentimento

**Objetivo.** Mudar o código do consumidor e ver a mudança chegar ao object storage: cada resultado vai para `positivas/` ou `negativas/`, conforme o sentimento que a API devolveu.

1. Abram o consumidor no editor com `code consumidor.py` e procurem a linha que monta o nome do blob. Ela tem um comentário logo acima avisando que é esta a linha da E1:

   ```python
   nome_blob = f"{args.prefixo}/{msg.partition()}-{msg.offset()}.json"
   ```

2. Troquem o prefixo fixo pelo sentimento. A API responde `positivo` ou `negativo`, no masculino, e os prefixos pedidos são `positivas` e `negativas`, no feminino: a linha precisa fazer essa tradução. Mantenham a partição e o offset no nome. Salvem com **Ctrl+S** e fechem com **Ctrl+Q**.
3. Rodem o consumidor com um grupo novo, lendo desde o início, para que o histórico inteiro passe pela linha nova:

   ```bash
   python3 consumidor.py --grupo e1-sentimento --desde-o-inicio --api --blob
   ```

   Cada linha da saída mostra o nome do blob gravado, que agora começa com `positivas/` ou `negativas/`. O histórico inteiro leva alguns minutos para passar, porque cada avaliação é uma chamada à API e uma gravação no Blob. Quando pararem de surgir linhas novas, **Ctrl+C**.
4. No portal, abram o container `avaliacoes`.

**A captura mostra** o trecho alterado do `consumidor.py` (no editor, ou com `grep -n -B1 -A1 "nome_blob =" consumidor.py`) e o container `avaliacoes` no portal com os dois prefixos, `positivas/` e `negativas/`, cheios de blobs.

### E2 · Dois consumidores no mesmo grupo

**Objetivo.** Ver o consumer group dividir as partições entre os membros, e o rebalanceamento acontecer quando um membro entra.

1. Na aba 1, deixem o produtor rodando: `python3 produtor.py`.
2. Na aba 2, um consumidor que só imprime:

   ```bash
   python3 consumidor.py --grupo e2-divisao
   ```

   Ele é o único membro e recebe as quatro partições.
3. Abram uma terceira aba com o botão **New session** da barra do Cloud Shell, entrem na pasta com `cd infra-para-ia/aula5-dados` e rodem **o mesmo comando, com o mesmo `--grupo`**.
4. O rebalanceamento leva alguns instantes. Na aba 2 aparece a linha `rebalanceamento: partições devolvidas ao grupo` e, em seguida, uma nova linha `partições atribuídas` com só uma parte das partições. A aba 3 recebe o resto. **Esperem a linha de partições atribuídas nas duas abas antes da captura.** Depois disso, cada aba passa a imprimir só as avaliações das próprias partições.

**A captura mostra** as duas abas com o mesmo `--grupo`, cada uma com as partições atribuídas depois do rebalanceamento, e duas linhas respondendo: **o que faria um quinto consumidor nesse grupo?**

### E3 · Nada se perde com o consumidor parado

**Objetivo.** Mostrar que os eventos esperam no log enquanto o consumidor está fora do ar, e que o grupo continua exatamente de onde parou.

1. Parem o produtor na aba 1 com **Ctrl+C**, ou esperem ele terminar o CSV.
2. Na aba 2, rodem o consumidor do grupo `pipeline` e deixem ele alcançar o fim do log:

   ```bash
   python3 consumidor.py --grupo pipeline --api --blob
   ```

   Quando pararem de surgir linhas novas, **Ctrl+C**. Agora o consumidor está parado e em dia.
3. Contem os blobs do container, trocando `<storage>` pelo nome da storage account da dupla:

   ```bash
   az storage blob list --account-name <storage> -c avaliacoes --query "length(@)" -o tsv
   ```

   Antes do número aparece um aviso amarelo dizendo que o comando vai buscar a chave da storage account. Ele não atrapalha: a contagem é o número da última linha.

4. Com o consumidor ainda parado, publiquem 20 avaliações na aba 1. O produtor termina com `fim: 20 avaliações enviadas`:

   ```bash
   python3 produtor.py --quantidade 20
   ```

5. Religuem o consumidor na aba 2, com o mesmo comando e o mesmo grupo. Ele processa as 20 avaliações que chegaram enquanto estava parado. Quando parar de imprimir, **Ctrl+C**.
6. Contem os blobs de novo, com o mesmo comando do passo 3.

A diferença entre as duas contagens tem que ser **exatamente 20**. O número absoluto não importa, porque o container também guarda o `avaliacoes.csv` da Etapa 2 e os blobs das outras evidências. O que importa é a diferença.

**A captura mostra** a contagem antes, a linha `fim: 20 avaliações enviadas`, o consumidor religado processando os eventos e a contagem depois, com exatamente 20 a mais.

### E4 · Replay do histórico

**Objetivo.** Reprocessar tudo o que está no log com um grupo novo, sem atrapalhar os grupos que já existem.

1. Escolham um nome de grupo **que nunca foi usado**, por exemplo `e4-replay1`. Um grupo que já existe continua do offset guardado e não relê nada.
2. Rodem, com o `consumidor.py` original:

   ```bash
   python3 consumidor.py --grupo e4-replay1 --desde-o-inicio --api --blob --prefixo reprocessado
   ```

   O consumidor percorre o histórico uma partição de cada vez: primeiro a partição 0 inteira, a partir do `off 0`, depois a 1, e assim por diante. O `off 0` no começo de cada partição mostra que o grupo começou do início do log. Os blobs vão para `reprocessado/`, e não para `resultados/`. Como na E1, o histórico inteiro leva alguns minutos.
3. Quando pararem de surgir linhas novas, **Ctrl+C**, e abram o container `avaliacoes` no portal.

O log guarda os eventos por 24 horas, lidos ou não. O histórico que volta é o dessas 24 horas.

**A captura mostra** o comando com o grupo novo, `--desde-o-inicio` e `--prefixo reprocessado`, as primeiras linhas da saída, com a partição 0 começando em `off 0`, o prefixo `reprocessado/` no portal com o histórico inteiro, e duas linhas respondendo: **por que isso importa quando sai uma versão nova do modelo?**

### E5 · Conta limpa

**Objetivo.** Terminar a atividade sem nada ligado.

```bash
az group delete -n aula5-rg --yes --no-wait
az group list -o table
```

Esperem alguns instantes e rodem o `az group list` de novo até o `aula5-rg` sumir. Aproveitem para apagar qualquer resource group que tenha sobrado das aulas anteriores. Se vocês usam o Cloud Shell com storage, o grupo `cloud-shell-storage-...` pode ficar.

**A captura mostra** o `az group list -o table` sem `aula5-rg` nem restos das aulas anteriores.

## Regra de consistência

O namespace e a storage account têm que ser os mesmos em todas as capturas, e os nomes seguem o apelido da dupla: `eh-<apelido>-aula5` e `st<apelido>aula5`. **Divergência invalida a evidência.**

As chaves do `config.env` nunca aparecem nas capturas. Não abram o `config.env` na tela na hora de capturar, e não rodem `cat config.env`.

## Dicas

- Quase todo erro é um valor do `config.env`. Abram o arquivo antes de procurar outra causa. A tabela **Erros comuns** do [roteiro](ROTEIRO.md) lista as mensagens dos scripts e o que fazer com cada uma.
- Se o `az storage blob list` reclamar de permissão, acrescentem `--auth-mode key` ao comando, como na aula 4.
- O consumidor só confirma o offset de uma avaliação depois de gravar o blob. Se a API falhar no meio, ele encerra sem confirmar, e a próxima execução com o mesmo grupo reprocessa o evento. Isso não quebra a E3: o blob é sobrescrito com o mesmo nome.
- Um grupo novo sem `--desde-o-inicio` começa do fim do log e fica esperando. Se o consumidor conecta e não imprime nada, confiram se o produtor está rodando.
- Custo: o namespace Standard com 1 throughput unit fica em torno de US$ 0,12 por hora, e a ACI em torno de US$ 0,07 por hora. Juntem as evidências em uma sessão só e apaguem tudo no fim.
