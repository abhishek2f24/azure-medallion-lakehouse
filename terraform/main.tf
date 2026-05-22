terraform {
  required_version = ">= 1.7.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
  backend "azurerm" {
    resource_group_name  = "tfstate-rg"
    storage_account_name = "tfstatelakehouse"
    container_name       = "tfstate"
    key                  = "lakehouse.terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
}

data "azurerm_client_config" "current" {}

# ── Resource Group ────────────────────────────────────────────────────────────
resource "azurerm_resource_group" "lakehouse" {
  name     = "${var.project_name}-${var.environment}-rg"
  location = var.location
  tags     = local.common_tags
}

# ── ADLS Gen2 ─────────────────────────────────────────────────────────────────
resource "azurerm_storage_account" "lakehouse" {
  name                     = "${replace(var.project_name, "-", "")}${var.environment}adls"
  resource_group_name      = azurerm_resource_group.lakehouse.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "ZRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Hierarchical namespace = ADLS Gen2

  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false  # Entra ID auth only

  blob_properties {
    delete_retention_policy {
      days = 30
    }
    versioning_enabled = true
  }

  tags = local.common_tags
}

# Medallion containers
resource "azurerm_storage_container" "medallion" {
  for_each              = toset(["bronze", "silver", "gold", "quarantine", "raw"])
  name                  = each.value
  storage_account_name  = azurerm_storage_account.lakehouse.name
  container_access_type = "private"
}

# Lifecycle policy — auto-tier Bronze to cool/archive
resource "azurerm_storage_management_policy" "lifecycle" {
  storage_account_id = azurerm_storage_account.lakehouse.id
  rule {
    name    = "bronze-tiering"
    enabled = true
    filters {
      prefix_match = ["bronze/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 30
        tier_to_archive_after_days_since_modification_greater_than = 90
      }
    }
  }
}

# ── Azure Key Vault ────────────────────────────────────────────────────────────
resource "azurerm_key_vault" "lakehouse" {
  name                = "${var.project_name}-${var.environment}-kv"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  purge_protection_enabled   = true
  soft_delete_retention_days = 90
  enable_rbac_authorization  = true

  tags = local.common_tags
}

# ── Azure Databricks ──────────────────────────────────────────────────────────
resource "azurerm_databricks_workspace" "lakehouse" {
  name                = "${var.project_name}-${var.environment}-dbw"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location
  sku                 = "premium"  # Required for Unity Catalog

  custom_parameters {
    no_public_ip             = true
    virtual_network_id       = azurerm_virtual_network.lakehouse.id
    public_subnet_name       = azurerm_subnet.databricks_public.name
    private_subnet_name      = azurerm_subnet.databricks_private.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
  }

  tags = local.common_tags
}

# ── Azure Data Factory ─────────────────────────────────────────────────────────
resource "azurerm_data_factory" "lakehouse" {
  name                = "${var.project_name}-${var.environment}-adf"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  github_configuration {
    account_name    = var.github_account
    branch_name     = "main"
    git_url         = "https://github.com"
    repository_name = var.github_repo
    root_folder     = "/ingestion/adf_pipelines"
  }

  tags = local.common_tags
}

# Grant ADF managed identity access to ADLS
resource "azurerm_role_assignment" "adf_adls" {
  scope                = azurerm_storage_account.lakehouse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.lakehouse.identity[0].principal_id
}

# ── Azure Purview ─────────────────────────────────────────────────────────────
resource "azurerm_purview_account" "lakehouse" {
  name                = "${var.project_name}-${var.environment}-purview"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

# ── Networking ─────────────────────────────────────────────────────────────────
resource "azurerm_virtual_network" "lakehouse" {
  name                = "${var.project_name}-${var.environment}-vnet"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location
  address_space       = ["10.0.0.0/16"]
  tags                = local.common_tags
}

resource "azurerm_subnet" "databricks_public" {
  name                 = "databricks-public"
  resource_group_name  = azurerm_resource_group.lakehouse.name
  virtual_network_name = azurerm_virtual_network.lakehouse.name
  address_prefixes     = ["10.0.1.0/24"]
  delegation {
    name = "databricks"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_subnet" "databricks_private" {
  name                 = "databricks-private"
  resource_group_name  = azurerm_resource_group.lakehouse.name
  virtual_network_name = azurerm_virtual_network.lakehouse.name
  address_prefixes     = ["10.0.2.0/24"]
  delegation {
    name = "databricks"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_network_security_group" "databricks" {
  name                = "databricks-nsg"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location
  tags                = local.common_tags
}

resource "azurerm_subnet_network_security_group_association" "public" {
  subnet_id                 = azurerm_subnet.databricks_public.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

resource "azurerm_subnet_network_security_group_association" "private" {
  subnet_id                 = azurerm_subnet.databricks_private.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

# ── Locals ─────────────────────────────────────────────────────────────────────
locals {
  common_tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
    owner       = "data-engineering"
  }
}
