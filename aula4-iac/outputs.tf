output "url_api" {
  description = "Endereço da API. Acrescente /docs, /versao ou /prediz."
  value       = "http://${azurerm_container_group.api.fqdn}:8000"
}

output "ip_api" {
  description = "IP público do container. Muda a cada recriação, ao contrário da URL."
  value       = azurerm_container_group.api.ip_address
}

output "resource_group" {
  description = "Nome do resource group criado."
  value       = azurerm_resource_group.rg.name
}

output "nome_cluster" {
  description = "Nome do cluster AKS."
  value       = azurerm_kubernetes_cluster.aks.name
}
