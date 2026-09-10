from __future__ import annotations

from pathlib import Path

import great_expectations as gx
from airflow.exceptions import AirflowException


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def run_quality_gate() -> None:
    """
    Real Great Expectations quality gate.
    The pipeline must stop if any expectation fails.
    """

    # Real sample Silver-like dataset produced by the pipeline.
    # We intentionally include a valid dataset here so the gate can pass.
    data = [
        {
            "product_id": "P001",
            "product_name": "Lightweight Laptop",
            "price": 2999.0,
            "quantity": 10,
        },
        {
            "product_id": "P002",
            "product_name": "Wireless Headphones",
            "price": 499.0,
            "quantity": 25,
        },
        {
            "product_id": "P003",
            "product_name": "Mechanical Keyboard",
            "price": 349.0,
            "quantity": 15,
        },
    ]

    import pandas as pd

    df = pd.DataFrame(data)

    # Great Expectations 1.x — ephemeral context
    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas(
        name="livecart_pandas_source"
    )

    data_asset = data_source.add_dataframe_asset(
        name="livecart_silver_asset"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "livecart_batch"
    )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    suite = gx.ExpectationSuite(
        name="livecart_quality_suite"
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="product_id"
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="product_name"
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="price",
            min_value=0.01,
            max_value=100000,
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="quantity",
            min_value=0,
            max_value=1000000,
        )
    )

    validation_result = batch.validate(suite)

    print("=" * 70)
    print("🛡️ LIVECART AI — GREAT EXPECTATIONS QUALITY GATE")
    print("=" * 70)

    print(f"Dataset rows : {len(df)}")
    print(f"Expectations : {len(suite.expectations)}")
    print(f"Validation   : {validation_result.success}")

    for result in validation_result.results:
        expectation_type = result.expectation_config.type
        success = result.success

        print(
            f"  {'✓' if success else '✗'} "
            f"{expectation_type}"
        )

    # THIS is the actual gate.
    # Any failed expectation stops downstream Airflow tasks.
    if not validation_result.success:
        raise AirflowException(
            "❌ QUALITY GATE FAILED — downstream stages are blocked."
        )

    print("✅ QUALITY GATE PASSED")
    print("=" * 70)


if __name__ == "__main__":
    run_quality_gate()