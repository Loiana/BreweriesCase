import os
import sys
import boto3
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO, StringIO
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
BUCKET_NAME_FROM = "silver-layer"         # Nome do bucket de origem
BUCKET_NAME_TO = "gold-layer"         # Nome do bucket de destino
FILE_NAME_TO = "aggregate_data"  # Nome do arquivo Parquet no bucket de destino


def read_parquet_files():

    # Conectar-se ao MinIO 
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_URL, 
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=boto3.session.Config(signature_version='s3v4')
    )

    try:
        # Listar objetos no bucket
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME_FROM)

        # Verificar se há arquivos no bucket
        if 'Contents' not in response:
            print(f"Nenhum arquivo encontrado no bucket '{BUCKET_NAME_FROM}'.")
            return []

        # Obter os nomes dos arquivos
        files = [obj['Key'] for obj in response['Contents']]
        
        return files

    except NoCredentialsError:
        print("Credenciais inválidas ou ausentes.")

    except Exception as e:
        print(f"Erro ao listar os arquivos no bucket: {e}")
        return []


def create_aggregated_view(files):

    # Conectar-se ao MinIO 
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_URL, 
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=boto3.session.Config(signature_version='s3v4')
    )

    dfs = []

    # Iterar sobre os arquivos Parquet
    for file_key in files:
        try:
            print(f"Lendo arquivo: {file_key}")
            # Obter o objeto do MinIO
            response = s3_client.get_object(Bucket=BUCKET_NAME_FROM, Key=file_key)
            
            # Ler o arquivo Parquet diretamente em um DataFrame
            df = pd.read_parquet(BytesIO(response['Body'].read()))  # Necessário converter para BytesIO
            dfs.append(df)
        except Exception as e:
            print(f"Erro ao ler o arquivo {file_key}: {e}")
    
    if not dfs:
        raise ValueError("Nenhum arquivo Parquet foi carregado com sucesso.")

    # Concatenar todos os DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)

 
    # Agrupar por tipo e localização (considerando as colunas 'brewery_type' e 'country' no exemplo)
    aggregated_view = combined_df.groupby(['brewery_type', 'country']).size().reset_index(name='brewery_count')
    
    return aggregated_view


def save_parquet(api_data):

    csv_data = api_data.to_csv(index=False)

    # Conecta ao MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    # Salva o CSV
    s3.put_object(
        Bucket=BUCKET_NAME_TO,
        Key=FILE_NAME_TO,
        Body=csv_data,
        ContentType="text/csv",
    )

    print(f"Arquivo CSV salvo com sucesso no MinIO em {FILE_NAME_TO}!")



def main():
    print("Início")

    files = read_parquet_files()
    data = create_aggregated_view(files)

    if data is not None:
        save_parquet(data)

    print("Fim")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)
