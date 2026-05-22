"""
Unity Catalog setup: catalogs, schemas, grants, column-level security, row filters.
Run once per environment during infra provisioning.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo, SchemaInfo, SecurableType, PrivilegeAssignment, Privilege
)


CATALOG_NAME   = "supply_lakehouse"
STORAGE_ROOT   = "abfss://unity-catalog@supplylakehouseprodadls.dfs.core.windows.net"

SCHEMAS = {
    "bronze":     "Raw ingested data, append-only, no PII masking",
    "silver":     "Cleansed, validated, deduplicated data",
    "gold":       "Business-ready aggregates, star schema",
    "quarantine": "Failed validation records for investigation",
}

GRANTS = {
    "bronze": {
        "data-engineers":   [Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.MODIFY],
        "data-scientists":  [Privilege.USE_SCHEMA, Privilege.SELECT],
    },
    "silver": {
        "data-engineers":   [Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.MODIFY],
        "data-scientists":  [Privilege.USE_SCHEMA, Privilege.SELECT],
        "analysts":         [Privilege.USE_SCHEMA, Privilege.SELECT],
    },
    "gold": {
        "data-engineers":   [Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.MODIFY],
        "data-scientists":  [Privilege.USE_SCHEMA, Privilege.SELECT],
        "analysts":         [Privilege.USE_SCHEMA, Privilege.SELECT],
        "bi-consumers":     [Privilege.USE_SCHEMA, Privilege.SELECT],
        "exec-viewers":     [Privilege.USE_SCHEMA, Privilege.SELECT],
    },
    "quarantine": {
        "data-engineers":   [Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.MODIFY],
    },
}

# Columns that require masking in non-engineer contexts
PII_COLUMNS = {
    "silver.orders":    ["customer_id"],
    "silver.suppliers": ["supplier_name"],
    "gold.fct_supply_chain": ["customer_id"],
}


def get_client() -> WorkspaceClient:
    return WorkspaceClient()


def provision_catalog(w: WorkspaceClient) -> None:
    try:
        w.catalogs.get(CATALOG_NAME)
        print(f"[Unity] Catalog '{CATALOG_NAME}' already exists.")
    except Exception:
        w.catalogs.create(name=CATALOG_NAME, storage_root=STORAGE_ROOT)
        print(f"[Unity] Created catalog: {CATALOG_NAME}")

    # Grant USE CATALOG to all data groups
    w.grants.update(
        securable_type=SecurableType.CATALOG,
        full_name=CATALOG_NAME,
        changes=[
            PrivilegeAssignment(
                principal=group,
                privileges=[Privilege.USE_CATALOG]
            )
            for group in ["data-engineers", "data-scientists", "analysts", "bi-consumers", "exec-viewers"]
        ],
    )


def provision_schemas(w: WorkspaceClient) -> None:
    for schema_name, comment in SCHEMAS.items():
        full_name = f"{CATALOG_NAME}.{schema_name}"
        try:
            w.schemas.get(full_name)
            print(f"[Unity] Schema '{full_name}' already exists.")
        except Exception:
            w.schemas.create(catalog_name=CATALOG_NAME, name=schema_name, comment=comment)
            print(f"[Unity] Created schema: {full_name}")

        # Apply grants
        if schema_name in GRANTS:
            w.grants.update(
                securable_type=SecurableType.SCHEMA,
                full_name=full_name,
                changes=[
                    PrivilegeAssignment(principal=group, privileges=privs)
                    for group, privs in GRANTS[schema_name].items()
                ],
            )
            print(f"[Unity] Grants applied to {full_name}")


def apply_column_masking(w: WorkspaceClient) -> None:
    """
    Apply column-level security: mask PII for non-engineer roles.
    Uses dynamic view pattern (Unity Catalog row filters / column masks).
    """
    mask_function_sql = """
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.mask_pii(value STRING)
    RETURNS STRING
    RETURN CASE
        WHEN is_account_group_member('data-engineers') THEN value
        ELSE CONCAT(LEFT(value, 3), '***')
    END
    """

    for table_ref, columns in PII_COLUMNS.items():
        schema_name, table_name = table_ref.split(".")
        fn_sql = mask_function_sql.format(catalog=CATALOG_NAME, schema=schema_name)
        w.statement_execution.execute_statement(
            warehouse_id="${DATABRICKS_WAREHOUSE_ID}",
            statement=fn_sql,
        )
        for col in columns:
            alter_sql = f"""
            ALTER TABLE {CATALOG_NAME}.{schema_name}.{table_name}
            ALTER COLUMN {col}
            SET MASK {CATALOG_NAME}.{schema_name}.mask_pii
            """
            w.statement_execution.execute_statement(
                warehouse_id="${DATABRICKS_WAREHOUSE_ID}",
                statement=alter_sql,
            )
        print(f"[Unity] Column masking applied: {table_ref} → {columns}")


def main():
    w = get_client()
    provision_catalog(w)
    provision_schemas(w)
    apply_column_masking(w)
    print("[Unity] Catalog setup complete.")


if __name__ == "__main__":
    main()
