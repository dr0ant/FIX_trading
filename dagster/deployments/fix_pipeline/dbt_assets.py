from dagster import load_assets_from_dbt_project
import os

DBT_PROJECT_PATH = os.path.join(os.path.dirname(__file__), "pictet_fix_project")
DBT_PROFILES_PATH = os.path.expanduser("~/.dbt")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES_PATH,
)