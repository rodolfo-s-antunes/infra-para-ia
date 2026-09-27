variable "dupla" {
  description = "Apelido da dupla, só letras minúsculas e dígitos, de 3 a 12 caracteres. Vai para as tags e para o endereço da API."
  type        = string
  default     = "alan"

  validation {
    condition     = can(regex("^[a-z0-9]{3,12}$", var.dupla))
    error_message = "Use só letras minúsculas e dígitos, de 3 a 12 caracteres, sem espaço nem hífen. Edite terraform.tfvars."
  }
}

variable "turma" {
  description = "Identificação da turma, usada nas tags."
  type        = string
  default     = "2026-2"
}

variable "location" {
  description = "Região do Azure. brazilsouth é a região padrão da disciplina. Se o Azure recusar com RequestDisallowedByAzure, rode python3 check_azure.py na raiz do repositório e escolha outra."
  type        = string
  default     = "brazilsouth"
}

variable "imagem_tag" {
  description = "Tag da imagem ghcr.io/rodolfo-s-antunes/sentiment-api a executar."
  type        = string
  default     = "v2"

  validation {
    condition     = contains(["v1", "v2", "v3"], var.imagem_tag)
    error_message = "imagem_tag deve ser v1, v2 ou v3."
  }
}

variable "cpu" {
  description = "vCPUs do container."
  type        = number
  default     = 1
}

variable "memoria" {
  description = "Memória do container, em GB."
  type        = number
  default     = 1
}
variable "node_vm_size" {
  description = "Tamanho da VM do nó do AKS, igual ao usado nas Aulas 2 e 3."
  type        = string
  default     = "Standard_D2as_v7"
}

variable "node_count" {
  description = "Número de nós do node pool padrão do AKS."
  type        = number
  default     = 1
}

