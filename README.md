# Azure Medallion Lakehouse — Supply Chain Analytics

Production-grade cloud data platform on Azure: end-to-end supply chain analytics pipeline using ADF, ADLS Gen2, Databricks, Delta Lake, Unity Catalog, dbt, Great Expectations, and Microsoft Fabric — fully provisioned via Terraform.

![Azure](https://img.shields.io/badge/Azure-Primary%20Cloud-0078D4?style=flat-square&logo=microsoftazure)
![Databricks](https://img.shields.io/badge/Databricks-Delta%20Lake-FF3621?style=flat-square&logo=databricks)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?style=flat-square&logo=dbt)
![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?style=flat-square&logo=terraform)
![Great Expectations](https://img.shields.io/badge/DQ-Great%20Expectations-orange?style=flat-square)
![Microsoft Fabric](https://img.shields.io/badge/Serving-Microsoft%20Fabric-742774?style=flat-square&logo=microsoft)
![CI](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?style=flat-square&logo=githubactions)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                        │
│                                                                               │
│  Azure SQL DB        REST APIs         Blob Storage        SharePoint/Excel   │
│  (ERP orders)    (Supplier APIs)    (Inventory CSVs)     (Finance reports)   │
└──────┬───────────────────┬─────────────────┬────────────────────┬────────────┘
       │                   │                 │                    │
       └───────────────────▼─────────────────▼────────────────────▼
                           │
                ┌──────────▼──────────┐
                │  Azure Data Factory  │
                │                     │
                │  ● Copy Activities   │
                │  ● Mapping DataFlow  │
                │  ● Triggers (tumbl.) │
                │  ● IR on Databricks  │
                └──────────┬──────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│               AZURE DATA LAKE STORAGE GEN2                                    │
│                                                                               │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐           │
│  │   BRONZE layer   │  │   SILVER layer   │  │    GOLD layer    │           │
│  │                  │  │                  │  │                  │           │
│  │  Raw data as-is  │  │  Cleansed +      │  │  Business-ready  │           │
│  │  Parquet format  │  │  Validated +     │  │  Aggregated +    │           │
│  │  Partitioned by  │  │  Deduplicated    │  │  Star schema     │           │
│  │  ingestion_date  │  │  Delta Lake      │  │  Delta Lake      │           │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘           │
│                                                                               │
│  Governance: Unity Catalog · Azure Purview · RBAC · Column-level security   │
└──────────────────────────────────────────────────────────────────────────────┘
                           │
                ┌──────────▼──────────┐
                │  Azure Databricks   │
                │                     │
                │  ● PySpark jobs      │
                │  ● Delta Live Tables │
                │  ● Unity Catalog     │
                │  ● MLflow tracking   │
                └──────────┬──────────┘
                           │
                ┌──────────▼──────────┐
                │    dbt on           │
                │    Databricks SQL   │
                │                     │
                │  staging → int →    │
                │  marts (star schema)│
                │  50+ tests          │
                └──────────┬──────────┘
                           │
                ┌──────────▼──────────┐
                │ Great Expectations  │
                │                     │
                │  ● Bronze suite     │
                │  ● Silver suite     │
                │  ● Anomaly detection│
                └──────────┬──────────┘
                           │
┌──────────────────────────▼───────────────────────────────────────────────────┐
│                        SERVING LAYER                                          │
│                                                                               │
│  Microsoft Fabric    Azure Synapse    Power BI Embedded    Azure Analysis     │
│  (OneLake mirror)   (SQL Serverless)  (Exec dashboards)   Services (SSAS)   │
└──────────────────────────────────────────────────────────────────────────────┘
                           │
┌──────────────────────────▼───────────────────────────────────────────────────┐
│                     INFRASTRUCTURE & OPERATIONS                               │
│                                                                               │
│  Terraform (IaC)  │  GitHub Actions CI/CD  │  Azure Monitor  │  PagerDuty   │
│  Azure DevOps     │  Key Vault (secrets)   │  Grafana        │  Opsgenie    │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Domain: Supply Chain Analytics

Tracks the full supply chain lifecycle for a retail/manufacturing business:

| Entity | Description |
|---|---|
| `orders` | Customer purchase orders from ERP |
| `shipments` | Carrier tracking events and delivery status |
| `inventory` | Warehouse stock levels and movements |
| `suppliers` | Vendor master data and performance metrics |
| `products` | SKU catalogue with category hierarchy |

**Key business questions answered by the Gold layer:**
- Which suppliers have the highest on-time delivery rate?
- What is the inventory turnover rate by category?
- Where are the supply chain bottlenecks causing delivery SLA breaches?
- What is the order fulfillment cost by region and carrier?
- Which SKUs are at risk of stockout in the next 14 days?

---

## Project Structure

```
azure-medallion-lakehouse/
├── terraform/                    # Full Azure infrastructure as code
│   ├── main.tf                   # Resource group, ADLS, Databricks, ADF, Key Vault
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/
│       ├── adls/                 # ADLS Gen2 + containers + lifecycle policies
│       └── databricks/           # Workspace + clusters + Unity Catalog
├── ingestion/
│   ├── adf_pipelines/
│   │   └── ingest_supply_chain.json   # ADF pipeline ARM template
│   ├── api_ingestion.py          # REST API → Bronze (supplier feeds)
│   └── requirements.txt
├── processing/
│   ├── bronze/raw_to_bronze.py   # Schema enforcement, partitioning
│   ├── silver/bronze_to_silver.py # Dedup, validation, SCD Type 2
│   └── gold/silver_to_gold.py   # Star schema assembly
├── dbt/
│   ├── models/
│   │   ├── staging/              # Source-aligned, light transforms
│   │   ├── intermediate/         # Business logic, joins, enrichment
│   │   └── marts/                # Delivery-ready fact + dim tables
│   └── tests/                    # Custom singular tests
├── data_quality/
│   ├── expectations/             # GE expectation suites per layer
│   └── checkpoints/              # GE checkpoint configs
├── governance/
│   └── unity_catalog_setup.py   # Catalog, schema, grants automation
├── monitoring/
│   └── data_observability.py    # Freshness, volume, schema drift alerts
└── .github/workflows/
    ├── ci.yml                    # Lint → test → dbt compile
    └── terraform.yml             # Plan on PR, apply on main merge
```

---

## Quick Start

```bash
git clone https://github.com/Abhishek2f24/azure-medallion-lakehouse.git
cd azure-medallion-lakehouse

# 1. Provision Azure infrastructure
cd terraform
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars

# 2. Configure ingestion
cp ingestion/.env.example ingestion/.env
# Fill in: Azure SQL connection, API keys, storage account name

# 3. Trigger ADF pipeline (or run locally)
python ingestion/api_ingestion.py --env dev --date 2024-01-01

# 4. Run Databricks processing jobs
databricks jobs run-now --job-id <bronze_job_id>
databricks jobs run-now --job-id <silver_job_id>

# 5. Run dbt
cd dbt
dbt deps
dbt run --target dev
dbt test --target dev
dbt docs generate && dbt docs serve

# 6. Run data quality checks
cd data_quality
python -m great_expectations checkpoint run bronze_checkpoint
python -m great_expectations checkpoint run silver_checkpoint
```

---

## Infrastructure (Terraform)

Resources provisioned automatically:

| Resource | Purpose |
|---|---|
| Resource Group | Isolated environment boundary |
| ADLS Gen2 | Bronze / Silver / Gold containers |
| Azure Databricks | PySpark processing + Delta Lake |
| Azure Data Factory | Orchestrated ingestion pipelines |
| Azure Key Vault | Secrets — connection strings, tokens |
| Azure Purview | Data catalog + lineage |
| Log Analytics | Centralized monitoring |
| Private Endpoints | Network isolation for all services |

---

## Data Quality Framework

Two-layer quality enforcement:

**Layer 1 — Great Expectations (Bronze → Silver gate):**
- Row count anomaly detection (±20% from 7-day moving average)
- Null checks on all primary keys
- Value range validation (prices > 0, quantities > 0)
- Referential integrity across source systems

**Layer 2 — dbt tests (Silver → Gold gate):**
- `not_null` + `unique` on all surrogate keys
- `relationships` tests across fact/dim joins
- Custom singular tests: no negative inventory, delivery_date >= order_date

---

## Performance

| Layer | Row Volume | Processing Time | Freshness SLA |
|---|---|---|---|
| Bronze | ~50M rows/day | < 20 min | < 30 min |
| Silver | ~45M rows/day | < 35 min | < 60 min |
| Gold | ~5M aggregated rows | < 15 min | < 90 min |
| Power BI refresh | — | < 5 min | < 2 hours |

---

## Author

**Abhishek Kumar Maurya** — Senior Data Engineer  
[LinkedIn](https://linkedin.com/in/abhishek2f24) · [GitHub](https://github.com/Abhishek2f24) · abhishek2f24@gmail.com  
Open to remote Senior Data Engineer roles — US · UK · Australia · Europe
# azure-medallion-lakehouse
