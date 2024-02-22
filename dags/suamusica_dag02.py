from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add 
import pendulum
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import boto3

import os
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Acesse as variáveis de ambiente como você faria normalmente
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = 'suamusica-recomendation-system'

def generate_df_final(**kwargs):
    # Obter o valor da data da variável kwargs
    data_interval_end = kwargs['data_interval_end']

    # Construir o caminho para os arquivos CSV usando a data
    df_dim_path = f"./dags/dia={data_interval_end}/df_dimentional.csv"
    df_transactional_path = f"./dags/dia={data_interval_end}/df_transactional.csv"
    df_feat_artist_path = f"./dags/dia={data_interval_end}/df_feat_artist.csv"

    # Ler os DataFrames
    df_dim = pd.read_csv(df_dim_path)
    df_transactional = pd.read_csv(df_transactional_path)
    df_feat_artist = pd.read_csv(df_feat_artist_path)

    # Juntar os DataFrames usando a função merge
    df_merged = pd.merge(df_transactional, df_dim, on='id_tracks', how='inner')
    df_final_suamusica = pd.merge(df_merged, df_feat_artist, on='id_artist', how='inner')

    # Construir o caminho para o arquivo final CSV usando a data
    file_path = f'./coleta_dados/df_final_suamusica_{data_interval_end}.csv'

    # Salvar o DataFrame final em um arquivo CSV
    df_final_suamusica.to_csv(file_path, index=False)
    
    
def send_to_s3(access_key, secret_key, bucket_name, data_interval_end):

    file_path = './coleta_dados/df_final_suamusica_{}.csv'.format(data_interval_end)

    # Nome do objeto no S3 (sem o caminho .coleta_dados/)
    s3_object_name = 'df_final_suamusica_{}.csv'.format(data_interval_end)

    # Cria um cliente S3
    s3 = boto3.client('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key)

    # Faz o upload do arquivo com o novo nome do objeto
    s3.upload_file(file_path, bucket_name, s3_object_name)

    print('Arquivo CSV enviado com sucesso!')
    
with DAG(
    dag_id='suamusica_dag02',
    start_date=pendulum.datetime(2024, 2, 8, tz="UTC"),
    schedule_interval='0 0 * * 1' # CRON Expressions
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    gerate_df_final = PythonOperator(
        task_id='create_dim_content',
        python_callable=generate_df_final,
        op_kwargs={'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'},
        dag=dag
    )
    
    send_csv_to_s3 = PythonOperator(
        task_id='send_csv_to_s3',
        python_callable=send_to_s3,
        op_kwargs={'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}',
                   'access_key':access_key,
                   'secret_key':secret_key,
                   'bucket_name':bucket_name
                   },
        dag=dag
    )

    start >> gerate_df_final >> send_csv_to_s3 >> end 