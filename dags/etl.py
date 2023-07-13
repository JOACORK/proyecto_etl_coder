from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import json
import pandas as pd
from psycopg2.extras import execute_values
import redshift_connector
import numpy as np
import os

from email import message
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

import smtplib

def enviar(subject, body_text):
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('kalmbach.jr@gmail.com','akmqttztbrmjcfwf')
        #subject="ETL completado"
        #body_text="El proceso de ETL ha finalizado"
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('kalmbach.jr@gmail.com','kalmbach.jr@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')

default_args={
    'owner': 'JoacoRK',
    'start_date': datetime(2023,7,11)
}



path_dag = os.getcwd()

with open(f"{path_dag}/keys/credencial_bbdd.json") as file:
    credenciales = json.load(file)

redshift_conn = {
    "host": credenciales["host"],
    "port": credenciales["port"],
    "database": credenciales["database"],
    "username": credenciales["user"],
    "pwd": credenciales["password"]
}

default_args={
    'owner': 'JoaquinRK',
    'start_date': datetime(2023,6,19),
    'retries': 5,
    'retry_delay': timedelta(minutes=2) # 2 min de espera antes de cualquier re intento
}
BC_dag = DAG(
    dag_id='ligas_futbol',
    default_args=default_args,
    description='Agrega data de ligas de futbol de forma diaria',
    schedule_interval="@daily",
    catchup=False
)

"""
CONECTAR A BASE DE DATOS
"""

def consulta_ligas_api(exec_date):
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')

    with open(f"{path_dag}/keys/credencial_futbol.txt",'r') as file:
        api_key = file.read().strip()

    import http.client

    conn_api = http.client.HTTPSConnection("v3.football.api-sports.io")

    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': api_key
        }

    endpoint = "/leagues"

    conn_api.request("GET", endpoint, headers=headers)

    res = conn_api.getresponse()
    if res:
        print("Respuesta Exitosa!")
        data = res.read()
        respuesta_json =json.loads(data)
        print(respuesta_json)
        with open(path_dag+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "w") as json_file:
                   json.dump(respuesta_json, json_file)
    else:
         print("Ocurrió un error.")


def transformar_data(exec_date):
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')

    with open(path_dag+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
                   loaded_data = json.load(json_file)
    
    df_ligas = pd.json_normalize(loaded_data,record_path=["response"],meta=["get"])

    columna_con_lista = df_ligas.applymap(lambda x: isinstance(x, list)).any()

    # Modificamos tipo de dato de lista a json donde corresponda
    for columna in df_ligas.columns:
        if columna in columna_con_lista[columna_con_lista[:]==True] :
            df_ligas[columna] = df_ligas[columna].apply(lambda x: json.dumps(x[0]))
    df_ligas.to_csv(f"{path_dag}/processed_data/data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv", index=False, mode='a')


    
def conexion_redshift(exec_date):
    print(f"Conectandose a la BD en la fecha: {exec_date}") 
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"

    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        print(conn)
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)


from psycopg2.extras import execute_values
import psycopg2
import psycopg2.extras

def cargar_data(exec_date, table_name):
    print(f"Cargando data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')

    dataframe = pd.read_csv(path_dag + '/processed_data/' + "data_" + str(date.year) + '-' + str(date.month) + '-' + str(date.day) + '-' + str(date.hour) + ".csv")

    dataframe.columns = [str(columna).replace(".", "_") for columna in dataframe.columns]
    dtypes = dataframe.dtypes

    cols = [str(dtype).replace(".", "_") for dtype in dtypes.index]
    tipos = list(dtypes.values)
    type_map = {'int64': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR (5000)'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]

    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]

    # combina la definicion de columnas en la sentencia de crear tablas
    table_schema = f"""CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)}); """
    table_schema = table_schema.replace(".", "_")

    print("Insertando DF en REDSHIFT")
    url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')

    # Definir columnas
    columns= ['seasons','league_id','league_name','league_type','league_logo','country_name','country_code','country_flag','get']

    from psycopg2.extras import execute_values
    cur = conn.cursor()
    # Define the table name
    table_name = 'ligas_futbol'
    # Define the columns you want to insert data into
    columns = columns
    # Generate 
    values = [tuple(x) for x in dataframe.to_numpy()]
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    # Execute the INSERT statement using execute_values
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")    


    print('Proceso terminado')

# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=consulta_ligas_api,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# 3. Envio de data 
# 3.1 Conexion a base de datos
task_3= PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag
)

# 3.2 Envio final
task_4 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}","ligas_futbol"],
    dag=BC_dag,
)


task_5 = PythonOperator(
        task_id='dag_envio_completado',
        python_callable=enviar,
        op_args=["ETL completado","El proceso de ETL ha finalizado!"],
        dag=BC_dag,
)



# Definicion orden de tareas
task_1 >> task_2 >> task_3 >> task_4 >> task_5
