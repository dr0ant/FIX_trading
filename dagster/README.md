# WizeDagster
 All my dagster flows deployed on NAS

 # command to run update code on the docker container 
 1- update the file on the NAS then rebuild the image
 sudo docker-compose build docker_wize_obsidian_flow_code

 2- restart the container 
 sudo docker-compose up -d --no-deps docker_wize_obsidian_flow_code


# command to execute on the container to do the 2FA for Icloud

sudo docker exec -it wize_obsidian /bin/bash

root@fcec0f775e43:/opt/dagster/app# ls

Markdown_to_postgres.py  __pycache__  dagster_load_icloud_into_db.py  icloud_md_files_getter.py  
icloud_params.json  postgres_params.json

python3 icloud_md_files_getter.py 


# restart a specific container 

sudo docker-compose down docker_dagster_epub_to_audiobook_code && sudo docker-compose up --build -d docker_dagster_epub_to_audiobook_code

Ok where is is pushed ?       