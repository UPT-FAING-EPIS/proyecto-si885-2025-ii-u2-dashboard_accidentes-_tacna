provider "azurerm" {
  features {}
  subscription_id = var.suscription_id
}

# Resource Group para la base de datos
resource "azurerm_resource_group" "rg_db" {
  name     = "upt-incidentes-tacna"
  location = var.location
}

# Azure SQL Server
resource "azurerm_mssql_server" "sql" {
  name                         = "upt-incidentes-tacna"
  resource_group_name          = azurerm_resource_group.rg_db.name
  location                     = azurerm_resource_group.rg_db.location
  version                      = "12.0"
  administrator_login          = var.sqladmin_username
  administrator_password       = var.sqladmin_password
  minimum_tls_version          = "1.2"
  public_network_access_enabled = true
}

# Firewall rule para permitir acceso público
resource "azurerm_mssql_firewall_rule" "allow_all" {
  name             = "AllowAllIPs"
  server_id        = azurerm_mssql_server.sql.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "255.255.255.255"
}

# Base de datos Azure SQL - TIER GRATUITO SERVERLESS
resource "azurerm_mssql_database" "shorten" {
  name      = "incidentestacna"
  server_id = azurerm_mssql_server.sql.id

  # Configuración Serverless GRATUITA
  # General Purpose Serverless Gen5 con 2 vCores
  sku_name                    = "GP_S_Gen5_2"
  min_capacity                = 0.5
  max_size_gb                 = 32  # 32GB incluidos en el tier gratuito
  auto_pause_delay_in_minutes = 60  # Se pausa después de 60 min de inactividad
  
  zone_redundant              = false
  read_scale                  = false
  
  # Importante: Configurar collation compatible con español/internacional
  collation                   = "SQL_Latin1_General_CP1_CI_AS"
  
  tags = {
    Environment = "Free Tier"
    Tier        = "Serverless"
  }
}

# Outputs movidos a outputs.tf para evitar duplicación