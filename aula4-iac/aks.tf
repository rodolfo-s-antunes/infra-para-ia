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

    tags = local.tags
  }

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}
