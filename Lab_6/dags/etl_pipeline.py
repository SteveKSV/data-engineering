from airflow.sdk import dag, task
from datetime import datetime, timedelta
import pandas as pd
import json


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="etl_pipeline",
    description="Simple ETL pipeline: extract JSON → flatten → load to DataFrame",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "lab6"],
)
def etl_pipeline():

    @task()
    def extract() -> dict:
        """Generate nested JSON data (simulates raw data source)."""
        raw_data = {
            "metadata": {
                "source": "sensor_api",
                "version": "1.0",
                "generated_at": datetime.now().isoformat(),
            },
            "records": [
                {
                    "id": 1,
                    "info": {"name": "Alice", "age": 30},
                    "metrics": {"score": 95.5, "level": "A"},
                },
                {
                    "id": 2,
                    "info": {"name": "Bob", "age": 25},
                    "metrics": {"score": 78.0, "level": "B"},
                },
                {
                    "id": 3,
                    "info": {"name": "Carol", "age": 35},
                    "metrics": {"score": 88.3, "level": "A"},
                },
                {
                    "id": 4,
                    "info": {"name": "Dave", "age": 28},
                    "metrics": {"score": 61.7, "level": "C"},
                },
                {
                    "id": 5,
                    "info": {"name": "Eve", "age": 22},
                    "metrics": {"score": 99.1, "level": "A"},
                },
            ],
        }
        print(f"[EXTRACT] Generated {len(raw_data['records'])} records")
        print(f"[EXTRACT] Raw data:\n{json.dumps(raw_data, indent=2, ensure_ascii=False)}")
        return raw_data

    @task()
    def transform(raw_data: dict) -> list:
        """Flatten nested JSON — reduce nesting depth to a single level."""
        flat_records = []

        for record in raw_data["records"]:
            flat = {
                "id": record["id"],
                "name": record["info"]["name"],
                "age": record["info"]["age"],
                "score": record["metrics"]["score"],
                "level": record["metrics"]["level"],
                "source": raw_data["metadata"]["source"],
                "generated_at": raw_data["metadata"]["generated_at"],
            }
            flat_records.append(flat)

        print(f"[TRANSFORM] Flattened {len(flat_records)} records")
        print(f"[TRANSFORM] Sample record:\n{json.dumps(flat_records[0], indent=2, ensure_ascii=False)}")
        return flat_records

    @task()
    def load(flat_records: list) -> None:
        """Create a pandas DataFrame and print it to console."""
        df = pd.DataFrame(flat_records)

        # Basic type casting
        df["score"] = df["score"].astype(float)
        df["age"] = df["age"].astype(int)

        print("\n[LOAD] ===== Final DataFrame =====")
        print(df.to_string(index=False))
        print(f"\n[LOAD] Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"[LOAD] Columns: {list(df.columns)}")
        print(f"[LOAD] Average score: {df['score'].mean():.2f}")
        print(f"[LOAD] Level distribution:\n{df['level'].value_counts().to_string()}")

    # Define task dependencies
    raw = extract()
    flat = transform(raw)
    load(flat)


etl_pipeline()