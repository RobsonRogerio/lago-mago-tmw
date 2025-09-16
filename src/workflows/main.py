# %%

import os
import dotenv
import requests
import json
from pathlib import Path

# %%

# Caminho relativo
env_path = Path(__file__).parent / '.env'
dotenv.load_dotenv(env_path, override=True)

DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', '').replace('https://', '').rstrip('/')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')

# %%

def list_job_names():
    return [i.replace('.json', '') for i in os.listdir('.') if i.endswith('.json')]

def load_settings(job_name):
    with open(f'{job_name}.json', 'r') as f: 
        settings = json.load(f)
    return settings

def reset_job(settings):
    url = f'https://{DATABRICKS_HOST}/api/2.2/jobs/reset'
    header = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    resp = requests.post(url=url, headers=header, json=settings)
    return resp

def main():
    for i in list_job_names():
        settings = load_settings(i)
        resp = reset_job(settings)
        if resp.status_code == 200:
            print(f'Job {i} atualizado com sucesso!')
        else:
            print(f'Erro ao atualizar job {i}: {resp.text}')

if __name__ == '__main__':
    main()
    
# %%
