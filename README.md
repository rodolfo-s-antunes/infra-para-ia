# Infraestrutura Computacional para IA

Repositório de artefatos das atividades práticas da disciplina de extensão **Infraestrutura Computacional para IA** (curso de extensão em IA Generativa, Unisinos).

Todas as práticas rodam no **Microsoft Azure**, usando apenas o navegador (portal + Cloud Shell). Nada precisa ser instalado no seu computador.

## Estrutura

| Pasta | Aula | Tema |
|---|---|---|
| [`aula1-containers/`](aula1-containers/) | 1 | Containers: build e execução de uma API de IA (ACR + ACI) |
| [`aula2-kubernetes/`](aula2-kubernetes/) | 2 | Kubernetes I: fundamentos, Pod e Service no AKS |
| [`aula3-kubernetes/`](aula3-kubernetes/) | 3 | Kubernetes II: réplicas, Deployment, rolling update e rollback |
| [`aula4-iac/`](aula4-iac/) | 4 | Infraestrutura como Código: Terraform no Cloud Shell e por pull request |
| [`aula5-dados/`](aula5-dados/) | 5 | Storage e streaming: Blob Storage e Event Hubs (endpoint Kafka), com um produtor e um consumidor em Python e a API de sentimento classificando avaliações em tempo real |

Custo estimado da prática da aula 5: cerca de US$ 0,40 por dupla, quase todo no namespace do Event Hubs, que cobra por hora enquanto existir.

## Antes da aula 1

1. Crie sua conta gratuita do Azure (siga o tutorial enviado por e-mail). Faça isso **na semana da aula 1** — o crédito de US$ 200 vale por 30 dias.
2. Consiga fazer login em [portal.azure.com](https://portal.azure.com).

## Descobrir o que a sua assinatura libera

Nem toda assinatura deixa criar recursos em qualquer região, e nem todo tamanho de VM tem cota disponível. O `check_azure.py` na raiz descobre quais regiões a sua assinatura libera, se AKS e ACI existem em cada uma (e, para a aula 5, Event Hubs e storage accounts) e quais tamanhos de VM têm cota de vCPU disponível. No fim, recomenda região e tamanho e mostra o que trocar em cada aula, da 2 à 5. Rode `python3 check_azure.py` no Cloud Shell, a partir da raiz do repositório. Para VMs comuns, fora do AKS, use `python3 check_azure.py --noaks`. O script só lê informações, não cria nem apaga nada.

## O fio condutor do curso

Uma mesma aplicação de IA (uma API de análise de sentimento) atravessa as 5 aulas: nasce em um container, entra em um cluster Kubernetes, ganha réplicas e troca de versão sem queda, vira código com Terraform e, por fim, lê dados do object storage e processa eventos em tempo real com Kafka.

## Imagens publicadas

Todas as imagens são construídas pelo GitHub Actions (pasta [`.github/workflows/`](.github/workflows/)) e publicadas no GitHub Container Registry, públicas:

| Imagem | Aula | Origem |
|---|---|---|
| `ghcr.io/rodolfo-s-antunes/sentiment-api:v1` | 1 | [`aula1-containers/`](aula1-containers/) |
| `ghcr.io/rodolfo-s-antunes/sentiment-api:v2` | 2 e 3 | [`aula2-kubernetes/api/`](aula2-kubernetes/api/), `VERSAO=v2` |
| `ghcr.io/rodolfo-s-antunes/sentiment-api:v3` | 3 | [`aula2-kubernetes/api/`](aula2-kubernetes/api/), `VERSAO=v3` |

A aula 4 não constrói imagem nova: o Terraform aponta para as mesmas tags `v2` e `v3` usadas nas aulas 2 e 3. A aula 5 também não: a API sobe no ACI direto da tag `v3`.

## Regra de ouro

Ao final de cada prática, **apague o resource group da aula** (`az group delete`). Infraestrutura de aula não dorme ligada.
