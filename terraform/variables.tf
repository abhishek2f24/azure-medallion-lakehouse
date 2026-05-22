variable "project_name" {
  description = "Project identifier used in all resource names"
  type        = string
  default     = "supply-lakehouse"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Must be dev, staging, or prod."
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "uksouth"
}

variable "github_account" {
  description = "GitHub account name for ADF source control"
  type        = string
  default     = "Abhishek2f24"
}

variable "github_repo" {
  description = "GitHub repo name for ADF source control"
  type        = string
  default     = "azure-medallion-lakehouse"
}

variable "databricks_cluster_node_type" {
  description = "Databricks worker node VM type"
  type        = string
  default     = "Standard_D8ds_v5"
}

variable "databricks_min_workers" {
  type    = number
  default = 2
}

variable "databricks_max_workers" {
  type    = number
  default = 10
}
