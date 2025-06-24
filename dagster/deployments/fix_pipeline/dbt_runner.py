import subprocess
from dagster import op

@op
def run_dbt_model():
    subprocess.run(["dbt", "run", "--select", "your_model_name"], check=True)
