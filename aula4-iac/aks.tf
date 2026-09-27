# O cluster AKS da Aula 3, agora como arquivo revisado no Git em vez de
# um `az aks create` digitado no terminal. Vive no mesmo aula4-rg da
# Container Instance, criado em main.tf.
resource "azurerm_kubernetes_cluster" "aks" {
  name                = "aks-${var.dupla}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  dns_prefix          = "aks-${var.dupla}"
  sku_tier            = "Free"

  default_node_pool {
    name       = "default"
    node_count = var.node_count
    vm_size    = var.node_vm_size
  }

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}