from dagster import Definitions, define_asset_job
from fix_sensor import process_new_files, new_fix_file_sensor, MinioResource, PostgresResource
from dbt_assets import dbt_assets

# Job qui enchaîne ingestion + dbt
from dagster import job

@job
def fix_ingest_and_dbt_job():
    process_new_files()
    dbt_assets()

defs = Definitions(
    assets=[dbt_assets],
    jobs=[fix_ingest_and_dbt_job],
    sensors=[new_fix_file_sensor],
    resources={
        "minio": MinioResource(
            endpoint_url="http://minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
        ),
        "postgres": PostgresResource(
            host="postgres",
            dbname="fix_db",
            user="admin",
            password="admin",
        ),
    },
)