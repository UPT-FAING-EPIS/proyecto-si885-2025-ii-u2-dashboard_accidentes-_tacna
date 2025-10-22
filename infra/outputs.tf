output "resource_group_db_name" {
  description = "Nombre del grupo de recursos para la base de datos"
  value       = azurerm_resource_group.rg_db.name
}

output "sql_server_name" {
  description = "Nombre del servidor Azure SQL Server"
  value       = azurerm_mssql_server.sql.name
}

output "sql_server_fqdn" {
  description = "Nombre DNS completo del servidor Azure SQL Server (usado para conexiones)"
  value       = azurerm_mssql_server.sql.fully_qualified_domain_name
}

output "sql_server_location" {
  description = "Región donde se desplegó el servidor SQL Server"
  value       = azurerm_mssql_server.sql.location
}

output "database_name" {
  description = "Nombre de la base de datos creada"
  value       = azurerm_mssql_database.shorten.name
}

output "database_sku" {
  description = "SKU de la base de datos (tier gratuito serverless)"
  value       = azurerm_mssql_database.shorten.sku_name
}

output "database_max_size_gb" {
  description = "Almacenamiento máximo asignado en GB (32GB incluidos en tier gratuito)"
  value       = azurerm_mssql_database.shorten.max_size_gb
}

output "connection_string" {
  description = "Connection string para conectarse a la base de datos (usar para aplicaciones)"
  value       = "Server=tcp:${azurerm_mssql_server.sql.fully_qualified_domain_name},1433;Initial Catalog=${azurerm_mssql_database.shorten.name};Persist Security Info=False;User ID=${var.sqladmin_username};Password=${var.sqladmin_password};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
  sensitive   = true
}

output "sqlcmd_connection" {
  description = "Comando para conectarse usando sqlcmd desde terminal"
  value       = "sqlcmd -S ${azurerm_mssql_server.sql.fully_qualified_domain_name} -d ${azurerm_mssql_database.shorten.name} -U ${var.sqladmin_username} -P '${var.sqladmin_password}'"
  sensitive   = true
}

output "azure_data_studio_server" {
  description = "Servidor para conectar desde Azure Data Studio o SQL Server Management Studio"
  value       = azurerm_mssql_server.sql.fully_qualified_domain_name
}