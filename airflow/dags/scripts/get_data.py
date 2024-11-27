import requests
import json
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
import yaml

#Busca parametros de configuracao
CONFIG_PATH = "/opt/airflow/config/config.yaml"
with open(CONFIG_PATH, 'r') as file:
    config = yaml.safe_load(file)

# Acessar valores
API_URL = config['default']['API_URL']
MINIO_URL = config['default']['MINIO_URL']
ACCESS_KEY = config['default']['ACCESS_KEY']
SECRET_KEY = config['default']['SECRET_KEY']

# Mais configuracoes do MinIO
BUCKET_NAME = "bronze-layer" 
FILE_NAME = "raw_data.csv" 


def request_api():

    try:
        # Faz a requisição HTTP
        response = requests.get(API_URL)
        
        # Levanta uma exceção se o código de status indicar erro
        response.raise_for_status()
        
        # Tenta converter o conteúdo para JSON
        api_data = response.json()

        api_data = pd.DataFrame(api_data)

        return api_data

    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
 
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")

    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")

    except requests.exceptions.RequestException as err:
        print(f"An Error Occurred: {err}")

    # Retorna None em caso de falha
    return None 


def save_csv(api_data):

    #Converte dados para CSV
    csv_data = api_data.to_csv(index=False)

    # Conecta ao MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    # Salva o CSV no MinIO
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=FILE_NAME,
        Body=csv_data,
        ContentType="text/csv",
    )

    print(f"Arquivo CSV salvo com sucesso no MinIO em {FILE_NAME}!")


def main():

    print("Início")

    data = request_api()
    #print(data)

    if data is not None:
        save_csv(data)

    print("Fim")


if __name__ == "__main__":

    try:
        main()

    except Exception as e:
        print(f"Critical error: {e}")

        sys.exit(1)