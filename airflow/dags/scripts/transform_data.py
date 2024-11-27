from io import BytesIO, StringIO
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import tempfile
import os
import shutil
from io import StringIO  
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
BUCKET_NAME_FROM = "bronze-layer"         # Nome do bucket de origem
BUCKET_NAME_TO = "silver-layer"         # Nome do bucket de destino
FILE_NAME_FROM = "raw_data.csv"          # Caminho e nome do arquivo no bucket de origem
FILE_NAME_TO = "transformed_data"  # Nome do arquivo Parquet no bucket de destino

PARTITIONING_COLUMN = "country" #city, state_province, postal_code, country, state


def read_data():
    # Conectar-se ao MinIO 
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_URL, 
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=boto3.session.Config(signature_version='s3v4')
    )


    try:
        # Ler o objeto do MinIO
        response = s3_client.get_object(Bucket=BUCKET_NAME_FROM, Key=FILE_NAME_FROM)
        
        # O conteúdo do objeto pode ser acessado através de 'Body'
        data = response['Body'].read() 
        
        # Converte os dados de bytes para string (UTF-8)
        data = data.decode('utf-8')

        #print("Ler o CSV com pandas")
        # Ler o CSV com pandas

        # Converte a string para um DataFrame pandas
        df = pd.read_csv(StringIO(data))

        return df


    except NoCredentialsError:
        print("Credenciais inválidas ou ausentes.")
    except Exception as e:
        print(f"Erro ao acessar o objeto: {e}")
    return None

def save_parquet(df):

    # Verificar se a coluna de particionamento existe
    if PARTITIONING_COLUMN not in df.columns:
        raise ValueError(f"'{PARTITIONING_COLUMN}' não encontrada no DataFrame.")

    # Criar uma tabela PyArrow do DataFrame
    table = pa.Table.from_pandas(df)

    # Iterar sobre os valores únicos de 'PARTITIONING_COLUMN' (ou outro campo de particionamento)
    for location in df[PARTITIONING_COLUMN].unique():
        # Criar o nome do arquivo com sufixo (com base na localização)
        file_name_with_suffix = f"{FILE_NAME_TO}_{location.replace(' ', '_')}.parquet"

        # Filtrar os dados para a localização específica
        df_location = df[df[PARTITIONING_COLUMN] == location]

        # Criar uma tabela PyArrow do DataFrame filtrado
        table_location = pa.Table.from_pandas(df_location)

        # Criar o buffer em memória para Parquet
        parquet_buffer = BytesIO()

        # Usando PyArrow para gravar o Parquet no buffer
        pq.write_table(table_location, parquet_buffer)

        # Posicionar o ponteiro do buffer no início antes de enviar
        parquet_buffer.seek(0)

        # Conectar-se novamente ao MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=boto3.session.Config(signature_version='s3v4')
        )

        # Enviar o arquivo Parquet para o MinIO com o sufixo no nome
        s3_client.put_object(
            Bucket=BUCKET_NAME_TO,
            Key=file_name_with_suffix,
            Body=parquet_buffer,
            ContentType='application/octet-stream'
        )

        print(f'Arquivo Parquet {file_name_with_suffix} salvo com sucesso no MinIO!')


def main():
    print("Início")

    data = read_data()
    if data is not None:
        save_parquet(data)

    print("Fim")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)
