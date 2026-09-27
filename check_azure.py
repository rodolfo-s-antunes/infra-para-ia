#!/usr/bin/env python3
"""
check_azure.py: descobre onde a sua assinatura do Azure deixa criar AKS e ACI
e, para a aula 5, Event Hubs e storage accounts.

Uso, no Cloud Shell (Bash), a partir da raiz do repositório:

    python3 check_azure.py                 # regiões liberadas pela política da assinatura
    python3 check_azure.py brazilsouth eastus   # só as regiões indicadas
    python3 check_azure.py --rapido        # pula a consulta de cotas (mais rápido)
    python3 check_azure.py --noaks         # VMs comuns: ignora AKS/ACI e lista qualquer tamanho com cota

O script só lê informações. Não cria, altera nem apaga nada.
"""
import argparse
import json
import subprocess
import sys

# Regiões usadas quando a assinatura não tem política de regiões e nenhuma foi indicada.
REGIOES_PADRAO = ["eastus", "eastus2", "brazilsouth", "northcentralus", "westus2", "westeurope"]

# Limites do nó do AKS usados nas aulas: 2 vCPU e pelo menos 4 GB de memória.
VCPU_MIN, VCPU_MAX, MEM_MIN_GB = 2, 4, 4

# Famílias que não servem para o nó da aula (GPU, HPC, memória alta etc.).
FAMILIAS_IGNORADAS = ("Standard_N", "Standard_H", "Standard_M", "Standard_L", "Standard_G", "Standard_F", "Standard_A")


def az(*args):
    """Roda um comando az e devolve o JSON. Em erro, devolve None e guarda a mensagem."""
    cmd = ["az", *args, "-o", "json"]
    try:
        saida = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
        return json.loads(saida) if saida.strip() else None
    except subprocess.CalledProcessError as e:
        az.ultimo_erro = e.stderr.strip().splitlines()[-1] if e.stderr.strip() else "erro desconhecido"
        return None
    except FileNotFoundError:
        sys.exit("Não encontrei o comando az. Rode este script no Cloud Shell ou instale o Azure CLI.")


az.ultimo_erro = ""


def regioes_permitidas():
    """Lê a política 'Allowed locations' da assinatura. Devolve a lista ou None se não houver."""
    atribuicoes = az("policy", "assignment", "list", "--disable-scope-strict-match") or []
    for a in atribuicoes:
        params = a.get("parameters") or {}
        for nome, valor in params.items():
            if "location" in nome.lower() and isinstance(valor, dict) and isinstance(valor.get("value"), list):
                return [r.lower() for r in valor["value"]]
    return None


def regioes_do_provider(namespace, tipo):
    """Regiões em que um tipo de recurso está disponível para esta assinatura."""
    prov = az("provider", "show", "--namespace", namespace)
    if not prov:
        return set()
    for rt in prov.get("resourceTypes", []):
        if rt.get("resourceType", "").lower() == tipo.lower():
            return {loc.lower().replace(" ", "") for loc in rt.get("locations", [])}
    return set()


def nomes_de_exibicao():
    """Mapa nome_interno -> nome de exibição das regiões (eastus -> East US)."""
    locs = az("account", "list-locations") or []
    return {l["name"].lower(): l.get("displayName", l["name"]) for l in locs}


def tamanhos_de_vm(regiao, so_para_aks=True):
    """Tamanhos de VM sem restrição na região.

    Com so_para_aks=True aplica os limites do nó da aula (2 a 4 vCPU, 4 GB, sem as
    famílias ignoradas). Com False devolve qualquer tamanho que a assinatura pode criar.
    """
    skus = az("vm", "list-skus", "--location", regiao, "--resource-type", "virtualMachines") or []
    resultado = []
    for s in skus:
        nome = s.get("name", "")
        if s.get("restrictions"):
            continue
        if so_para_aks and (nome.startswith(FAMILIAS_IGNORADAS) or "Promo" in nome):
            continue
        caps = {c["name"]: c["value"] for c in s.get("capabilities", [])}
        try:
            vcpus = int(caps.get("vCPUs", 0))
            mem = float(caps.get("MemoryGB", 0))
        except ValueError:
            continue
        if so_para_aks and not (VCPU_MIN <= vcpus <= VCPU_MAX and mem >= MEM_MIN_GB):
            continue
        resultado.append({"nome": nome, "vcpus": vcpus, "mem": mem, "familia": s.get("family", "")})
    return resultado


def cotas(regiao):
    """Cota de vCPU por família e o total regional. Devolve {familia: (usado, limite)}."""
    uso = az("vm", "list-usage", "--location", regiao) or []
    tabela = {}
    for u in uso:
        nome = (u.get("name") or {}).get("value", "")
        try:
            tabela[nome] = (int(u.get("currentValue", 0)), int(u.get("limit", 0)))
        except (TypeError, ValueError):
            pass
    return tabela


def ordenar_tamanhos(tamanhos, so_para_aks=True):
    """Para o AKS prefere as séries D 'as' e 's' de 2 vCPU, as mais baratas para o nó da aula.
    Para VMs comuns ordena do menor para o maior (vCPU, memória, nome)."""
    def chave(t):
        n = t["nome"]
        if not so_para_aks:
            return (t["vcpus"], t["mem"], n)
        serie = 0 if "_D2a" in n else 1 if "_D2" in n else 2 if "_B2" in n else 3 if "_D4" in n else 4
        return (t["vcpus"], serie, -t["mem"], n)
    return sorted(tamanhos, key=chave)


def main():
    ap = argparse.ArgumentParser(description="Onde a sua assinatura deixa criar AKS e ACI.")
    ap.add_argument("regioes", nargs="*", help="regiões a verificar (padrão: as liberadas pela política)")
    ap.add_argument("--rapido", action="store_true", help="não consultar cotas de vCPU (lista todos os tamanhos liberados, sem filtrar)")
    ap.add_argument("--noaks", action="store_true", help="VMs comuns: ignora AKS e ACI e lista qualquer tamanho de VM com cota, não só os do nó da aula")
    args = ap.parse_args()
    aks = not args.noaks

    conta = az("account", "show")
    if not conta:
        sys.exit("Não consegui ler a assinatura. Rode az login e tente de novo.\n" + az.ultimo_erro)
    print(f"Assinatura: {conta.get('name')}  ({conta.get('id')})")

    politica = regioes_permitidas()
    if args.regioes:
        regioes = [r.lower() for r in args.regioes]
        origem = "indicadas na linha de comando"
    elif politica:
        regioes = politica
        origem = "liberadas pela política da assinatura"
    else:
        regioes = REGIOES_PADRAO
        origem = "lista padrão do script (a assinatura não tem política de regiões)"
    regioes = sorted(dict.fromkeys(regioes))
    print(f"Regiões {origem}: {', '.join(regioes)}")
    if politica and args.regioes:
        fora = [r for r in regioes if r not in politica]
        if fora:
            print(f"Atenção: a política da assinatura NÃO libera {', '.join(fora)}. O Azure vai recusar recursos lá.")

    if not aks:
        print("Modo --noaks: procurando regiões e tamanhos para VMs comuns, sem olhar AKS nem ACI.")
    print("Consultando o Azure, isso leva um pouco por região...\n")
    if aks:
        aks_regioes = regioes_do_provider("Microsoft.ContainerService", "managedClusters")
        aci_regioes = regioes_do_provider("Microsoft.ContainerInstance", "containerGroups")
        eh_regioes = regioes_do_provider("Microsoft.EventHub", "namespaces")
        st_regioes = regioes_do_provider("Microsoft.Storage", "storageAccounts")
    else:
        vm_regioes = regioes_do_provider("Microsoft.Compute", "virtualMachines")
    exibicao = nomes_de_exibicao()

    recomendacoes = []
    aula5 = []  # regiões com ACI, Event Hubs e storage: a aula 5 não usa VM
    for regiao in regioes:
        titulo = f"{regiao}  ({exibicao.get(regiao, regiao)})"
        print("=" * 72)
        print(titulo)
        print("-" * 72)
        if aks:
            tem_aks = regiao in aks_regioes
            tem_aci = regiao in aci_regioes
            print(f"  AKS disponível: {'sim' if tem_aks else 'NÃO'}    ACI disponível: {'sim' if tem_aci else 'NÃO'}")
            tem_eh, tem_st = regiao in eh_regioes, regiao in st_regioes
            print(f"  Event Hubs disponível: {'sim' if tem_eh else 'NÃO'}    Storage disponível: {'sim' if tem_st else 'NÃO'}")
            if tem_aci and tem_eh and tem_st:
                aula5.append(regiao)
        else:
            tem_aks = regiao in vm_regioes
            tem_aci = False
            print(f"  VMs disponíveis: {'sim' if tem_aks else 'NÃO'}")

        tamanhos = ordenar_tamanhos(tamanhos_de_vm(regiao, aks), aks)
        if not tamanhos:
            print("  Nenhum tamanho de VM de 2 a 4 vCPU liberado nesta região." if aks
                  else "  Nenhum tamanho de VM liberado nesta região.")
            if az.ultimo_erro:
                print(f"  ({az.ultimo_erro})")
            print()
            continue

        cota = {} if args.rapido else cotas(regiao)
        usado_total, limite_total = cota.get("cores", (None, None))
        livres_regiao = None
        if limite_total is not None:
            livres_regiao = limite_total - usado_total
            print(f"  vCPUs na região: {livres_regiao} livres de {limite_total}")

        # Só interessa ao aluno o tamanho que ele consegue criar: com cota livre
        # na família e no total da região. Os demais ficam de fora da lista.
        com_cota, sem_cota, sem_info = [], 0, 0
        for t in tamanhos:
            if not cota:
                com_cota.append((t, None))
                continue
            usado, limite = cota.get(t["familia"], (None, None))
            if limite is None:
                sem_info += 1
                continue
            disp = limite - usado
            if livres_regiao is not None:
                disp = min(disp, livres_regiao)
            if disp >= t["vcpus"]:
                com_cota.append((t, (disp, limite)))
            else:
                sem_cota += 1

        melhor = None
        if not com_cota:
            print("  Nenhum tamanho de VM com cota de vCPU disponível nesta região.")
            detalhes = []
            if sem_cota:
                detalhes.append(f"{sem_cota} tamanho(s) liberado(s) mas com a cota da família zerada ou esgotada")
            if sem_info:
                detalhes.append(f"{sem_info} sem informação de cota")
            if detalhes:
                print("  (" + ", ".join(detalhes) + ")")
            print()
            continue

        print(f"  {'Tamanho':<24}{'vCPU':>5}{'Mem GB':>8}   {'vCPU livres na família' if cota else ''}")
        for t, q in com_cota:
            livres = f"{q[0]} de {q[1]}" if q else ""
            print(f"  {t['nome']:<24}{t['vcpus']:>5}{t['mem']:>8.0f}   {livres}")
            if melhor is None:
                melhor = t
        if sem_cota or sem_info:
            fora = []
            if sem_cota:
                fora.append(f"{sem_cota} sem cota")
            if sem_info:
                fora.append(f"{sem_info} sem informação de cota")
            print(f"  Fora da lista: {', '.join(fora)}.")
        print()
        if melhor and tem_aks:
            recomendacoes.append((regiao, melhor["nome"], tem_aci))

    print("=" * 72)
    if aks:
        recomendar_aulas(recomendacoes, aula5, exibicao)
        return
    if not recomendacoes:
        print("Nenhuma região com tamanho de VM com cota livre. Fale com o professor.")
        return
    regiao, vm, _ = recomendacoes[0]
    print("RECOMENDAÇÃO")
    print(f"  Região: {regiao}    Menor tamanho com cota: {vm}")
    if len(recomendacoes) > 1:
        print("  Outras regiões: " + ", ".join(f"{r} ({v})" for r, v, _ in recomendacoes[1:]))
    print()
    print("  Para criar uma VM, use nos comandos:")
    print(f"    --location {regiao} --size {vm}")
    print("  A lista acima mostra os outros tamanhos com cota em cada região.")


def recomendar_aulas(recomendacoes, aula5, exibicao):
    """Imprime a recomendação de cada aula, na ordem das aulas.

    recomendacoes: [(regiao, tamanho, tem_aci)] das regiões com AKS e cota, a melhor primeiro.
    aula5: regiões com ACI, Event Hubs e storage. A aula 5 não usa VM, então não depende da cota.
    """
    print("RECOMENDAÇÃO")
    if recomendacoes:
        regiao, vm, tem_aci = recomendacoes[0]
        print(f"  Região: {regiao}    Tamanho do nó: {vm}")
        if len(recomendacoes) > 1:
            print("  Alternativas: " + ", ".join(f"{r} ({v})" for r, v, _ in recomendacoes[1:]))
        print()
        print("  Aulas 2 e 3 (AKS pelo Cloud Shell), troque nos comandos:")
        print(f"    --location {regiao} --node-vm-size {vm}")
        print()
        if tem_aci:
            print("  Aula 4 (Terraform), acrescente ao terraform.tfvars:")
            print(f'    location = "{regiao}"')
        else:
            aci = [r for r, _, a in recomendacoes if a]
            if aci:
                print(f"  Aula 4 (Terraform): esta região não tem ACI. Acrescente ao terraform.tfvars:")
                print(f'    location = "{aci[0]}"')
            else:
                print("  Aula 4 (Terraform): nenhuma das regiões liberadas tem ACI. Fale com o professor.")
    else:
        regiao = None
        print("  Aulas 2, 3 e 4: nenhuma região com AKS e tamanho de VM com cota livre. Fale com o professor.")
    print()
    if not aula5:
        print("  Aula 5 (Event Hubs, Blob e ACI): nenhuma das regiões tem os três serviços. Fale com o professor.")
        return
    # A mesma região das outras aulas, quando ela tem os três serviços; senão, a próxima alternativa que tenha
    r5 = next((r for r, _, _ in recomendacoes if r in aula5), aula5[0])
    if regiao is None or r5 == regiao:
        print("  Aula 5 (Event Hubs, Blob e ACI), troque nos comandos e no portal:")
    else:
        print("  Aula 5 (Event Hubs, Blob e ACI): esta região não tem os três serviços. Troque nos comandos e no portal:")
    print(f"    az group create -n aula5-rg -l {r5}")
    print(f"    Region e Location, no portal: {exibicao.get(r5, r5)}")

if __name__ == "__main__":
    main()
