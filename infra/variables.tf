variable "suscription_id" {
  description = "ID de la suscripción de Azure donde se desplegarán los recursos. Se obtiene desde el portal de Azure o con 'az account show'"
  type        = string
}

variable "location" {
  default     = "Brazil South"
  description = "Región de Azure donde se crearán los recursos. Brazil South es la región más cercana con tier gratuito disponible"
}

variable "sqladmin_username" {
  default     = "sqladmin"
  description = "Nombre de usuario administrador para el SQL Server. No puede ser 'admin', 'administrator', 'sa', 'root', 'dbmanager', 'loginmanager'"
  sensitive   = true
  
  validation {
    condition     = !contains(["admin", "administrator", "sa", "root", "dbmanager", "loginmanager"], lower(var.sqladmin_username))
    error_message = "El nombre de usuario no puede ser: admin, administrator, sa, root, dbmanager, o loginmanager"
  }
}

variable "sqladmin_password" {
  default     = "Password123!"
  description = "Contraseña para el administrador del SQL Server. Debe tener mínimo 8 caracteres con mayúsculas, minúsculas, números y caracteres especiales"
  sensitive   = true
  
  validation {
    condition     = length(var.sqladmin_password) >= 8
    error_message = "La contraseña debe tener al menos 8 caracteres"
  }
}