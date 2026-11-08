import os
from datetime import datetime, timezone

import requests
from pymongo import MongoClient, UpdateOne

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Frankfurter API configuration
API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY"]

# MongoDB configuration
MONGO_URI = os.environ["MONGO_URI"]
MONGO_DATABASE = "currency_db"
MONGO_COLLECTION = "currency_rates"


@dag(
    dag_id="currency_exchange_etl",
    description="Daily currency exchange rate ETL pipeline",
    schedule="@daily",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ETL", "currency", "MongoDB"],
)
def currency_exchange_etl():

    # TASK 1: Extract
    @task(task_id="extract_currency_rates")
    def extract_currency_rates():

        response = requests.get(
            API_URL,
            params={
                "base": BASE_CURRENCY,
                "quotes": ",".join(TARGET_CURRENCIES),
            },
            timeout=30,
        )

        response.raise_for_status()
        raw_data = response.json()

        # Named XCom: save the extracted API response
        context = get_current_context()
        ti = context["ti"]

        ti.xcom_push(
            key="raw_rates",
            value=raw_data,
        )

        print(f"Extracted {len(raw_data)} exchange rate records.")

    # TASK 2: Transform
    @task(task_id="transform_exchange_rate_data")
    def transform_exchange_rate_data():

        context = get_current_context()
        ti = context["ti"]

        # Named XCom: retrieve extracted data
        raw_data = ti.xcom_pull(
            task_ids="extract_currency_rates",
            key="raw_rates",
        )

        if not raw_data:
            raise ValueError("No exchange rate data was received.")

        retrieved_at = datetime.now(timezone.utc).isoformat()
        transformed_data = []

        for item in raw_data:
            transformed_record = {
                "base_currency": item["base"],
                "target_currency": item["quote"],
                "exchange_rate": str(item["rate"]),
                "rate_date": item["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }

            transformed_data.append(transformed_record)

        # Named XCom: save transformed records
        ti.xcom_push(
            key="transformed_rates",
            value=transformed_data,
        )

        print(
            f"Transformed {len(transformed_data)} exchange rate records."
        )

    # TASK 3: Load
    @task(task_id="load_data_into_mongodb")
    def load_data_into_mongodb():

        context = get_current_context()
        ti = context["ti"]

        # Named XCom: retrieve transformed data
        transformed_data = ti.xcom_pull(
            task_ids="transform_exchange_rate_data",
            key="transformed_rates",
        )

        if not transformed_data:
            raise ValueError("No transformed data was received.")

        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=10000,
        )

        try:
            # Check MongoDB connection
            client.admin.command("ping")

            collection = client[MONGO_DATABASE][MONGO_COLLECTION]

            # Avoid duplicate records when the DAG runs again
            operations = []

            for record in transformed_data:
                operations.append(
                    UpdateOne(
                        {
                            "base_currency": record["base_currency"],
                            "target_currency": record["target_currency"],
                            "rate_date": record["rate_date"],
                        },
                        {"$set": record},
                        upsert=True,
                    )
                )

            result = collection.bulk_write(operations)

            print(
                f"MongoDB load complete. "
                f"Matched: {result.matched_count}, "
                f"modified: {result.modified_count}, "
                f"inserted: {len(result.upserted_ids)}"
            )

        finally:
            client.close()

    # Define task dependencies
    extract_task = extract_currency_rates()
    transform_task = transform_exchange_rate_data()
    load_task = load_data_into_mongodb()

    extract_task >> transform_task >> load_task


# Instantiate the DAG
currency_exchange_etl()
