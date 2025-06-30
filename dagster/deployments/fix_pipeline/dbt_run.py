import subprocess
import os

def run_dbt_models(profile_dir, project_dir, target="dev", models=None):
    """
    Exécute dbt run avec le profile et le projet spécifiés.
    :param profile_dir: Chemin vers le dossier contenant profiles.yml
    :param project_dir: Chemin vers le dossier du projet dbt
    :param target: Nom du target à utiliser (dev ou container)
    :param models: Liste de modèles à exécuter (optionnel)
    """
    cmd = [
        "dbt", "run",
        "--profiles-dir", profile_dir,
        "--project-dir", project_dir,
        "--target", target
    ]
    if models:
        cmd += ["--select"] + models

    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(__file__)
    PROFILE_DIR = os.path.join(BASE_DIR, "pictet_fix_project", ".dbt")
    PROJECT_DIR = os.path.join(BASE_DIR, "pictet_fix_project")
    DBT_TARGET = os.environ.get("DBT_TARGET", "dev")
    run_dbt_models(PROFILE_DIR, PROJECT_DIR, target=DBT_TARGET)