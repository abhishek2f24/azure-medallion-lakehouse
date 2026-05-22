"""
Great Expectations suite for Bronze → Silver gate on orders.
Run via: great_expectations checkpoint run bronze_orders_checkpoint
"""

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint


def build_orders_suite(context: gx.DataContext) -> str:
    suite_name = "bronze_orders_suite"

    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.create_expectation_suite(suite_name)

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="delta_lake_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="bronze_orders",
            runtime_parameters={"path": "abfss://bronze@supplylakehouseprodadls.dfs.core.windows.net/orders"},
            batch_identifiers={"run_id": "validation_run"},
        ),
        expectation_suite_name=suite_name,
    )

    # ── Primary key integrity ─────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_be_unique("order_id")

    # ── Foreign key presence ──────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_not_be_null("supplier_id")
    validator.expect_column_values_to_not_be_null("product_id")

    # ── Business rules ────────────────────────────────────────────────────────
    validator.expect_column_values_to_be_between("quantity",   min_value=1)
    validator.expect_column_values_to_be_between("unit_price", min_value=0.01)
    validator.expect_column_values_to_be_between("total_value",min_value=0.01)

    validator.expect_column_values_to_be_in_set(
        "status",
        ["pending", "confirmed", "in_production", "shipped", "delivered", "cancelled"]
    )

    validator.expect_column_values_to_be_in_set(
        "currency",
        ["USD", "GBP", "EUR", "AUD", "CAD", "JPY", "INR"]
    )

    # ── Date logic ────────────────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("order_date")
    validator.expect_column_values_to_match_regex(
        "order_date", r"^\d{4}-\d{2}-\d{2}$"
    )

    # ── Volume anomaly detection ──────────────────────────────────────────────
    validator.expect_table_row_count_to_be_between(
        min_value=1000,
        max_value=5_000_000,
    )

    # ── Completeness thresholds ───────────────────────────────────────────────
    for col in ["region", "source_system"]:
        validator.expect_column_values_to_not_be_null(col, mostly=0.95)

    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"[GE] Suite '{suite_name}' saved with {len(suite.expectations)} expectations.")
    return suite_name


def run_checkpoint(context: gx.DataContext, suite_name: str) -> dict:
    checkpoint_config = {
        "name": "bronze_orders_checkpoint",
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "delta_lake_datasource",
                    "data_connector_name": "runtime_data_connector",
                    "data_asset_name": "bronze_orders",
                },
                "expectation_suite_name": suite_name,
            }
        ],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
            {
                "name": "send_slack_notification_on_failure",
                "action": {
                    "class_name": "SlackNotificationAction",
                    "slack_webhook": "${SLACK_WEBHOOK_URL}",
                    "notify_on": "failure",
                    "renderer": {"module_name": "great_expectations.render.renderer.slack_renderer"},
                },
            },
        ],
    }

    checkpoint = SimpleCheckpoint(
        name="bronze_orders_checkpoint",
        data_context=context,
        **checkpoint_config,
    )
    result = checkpoint.run()

    if not result.success:
        failed = [
            r.expectation_config.expectation_type
            for r in result.run_results.values()
            for r in r["validation_result"].results
            if not r.success
        ]
        raise ValueError(f"[GE] Validation FAILED. Failed expectations: {failed}")

    print("[GE] All expectations passed.")
    return result


if __name__ == "__main__":
    context = gx.get_context()
    suite_name = build_orders_suite(context)
    run_checkpoint(context, suite_name)
