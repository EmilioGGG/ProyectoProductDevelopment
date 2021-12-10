import os

from airflow import DAG
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from structlog import get_logger
import pandas as pd

logger = get_logger()

dag = DAG('PD_ProyectoFinal', description='Product Development - Proyecto Final',
          default_args={
              'owner': '21000203_21001985',
              'depends_on_past': False,
              'max_active_runs': 1,
              'start_date': days_ago(2)
          },
          schedule_interval='0 1 * * *',
          catchup=False)

sensorCC = FileSensor(task_id='CovidConfirmed_sensor_file',
                    dag=dag,
                    fs_conn_id='my_file_conection',
                    filepath='time_series_covid19_confirmed_global.csv',
                    poke_interval=5,
                    timeout=60
                    )

sensorCD = FileSensor(task_id='CovidDeaths_sensor_file',
                    dag=dag,
                    fs_conn_id='my_file_conection',
                    filepath='time_series_covid19_deaths_global.csv',
                    poke_interval=5,
                    timeout=60
                    )

sensorCR = FileSensor(task_id='CovidRecovered_sensor_file',
                    dag=dag,
                    fs_conn_id='my_file_conection',
                    filepath='time_series_covid19_recovered_global.csv',
                    poke_interval=5,
                    timeout=60
                    )


def process_fileCC():
    file_path = f"{FSHook('my_file_conection').get_path()}/time_series_covid19_confirmed_global.csv"
    confirmed_global = (pd.read_csv(file_path, encoding="ISO-8859-1"))
    confirmed_global = pd.melt(confirmed_global, id_vars=['Province/State', 'Country/Region', 'Lat', 'Long']).rename(
        columns={'variable': 'Covid_Date', 'value': 'Covid_Cases'}, inplace=False)

    connection = MySqlHook('mysql_default').get_sqlalchemy_engine()
    with connection.begin() as transaction:
        transaction.execute('DELETE FROM test.time_series_covid19_confirmed_global where 1=1')
        confirmed_global.to_sql('time_series_covid19_confirmed_global', con=transaction, schema='test', if_exists='append', index=False)

    logger.info(f'Rows Inserted {len(confirmed_global.index)}')
    os.remove(file_path)
    logger.info(f'File {file_path} was deleted.')

def process_fileCD():
    file_path = f"{FSHook('my_file_conection').get_path()}/time_series_covid19_deaths_global.csv"
    deaths_global = (pd.read_csv(file_path, encoding="ISO-8859-1"))
    deaths_global = pd.melt(deaths_global, id_vars=['Province/State', 'Country/Region', 'Lat', 'Long']).rename(
        columns={'variable': 'Covid_Date', 'value': 'Covid_Cases'}, inplace=False)

    connection = MySqlHook('mysql_default').get_sqlalchemy_engine()
    with connection.begin() as transaction:
        transaction.execute('DELETE FROM test.time_series_covid19_deaths_global where 1=1')
        deaths_global.to_sql('time_series_covid19_deaths_global', con=transaction, schema='test',
                                if_exists='append', index=False)

    logger.info(f'Rows Inserted {len(deaths_global.index)}')
    os.remove(file_path)
    logger.info(f'File {file_path} was deleted.')

def process_fileCR():
    file_path = f"{FSHook('my_file_conection').get_path()}/time_series_covid19_recovered_global.csv"
    recovered_global = (pd.read_csv(file_path, encoding="ISO-8859-1"))
    recovered_global = pd.melt(recovered_global, id_vars=['Province/State', 'Country/Region', 'Lat', 'Long']).rename(
        columns={'variable': 'Covid_Date', 'value': 'Covid_Cases'}, inplace=False)

    connection = MySqlHook('mysql_default').get_sqlalchemy_engine()
    with connection.begin() as transaction:
        transaction.execute('DELETE FROM test.time_series_covid19_recovered_global where 1=1')
        recovered_global.to_sql('time_series_covid19_recovered_global', con=transaction, schema='test',
                             if_exists='append', index=False)

    logger.info(f'Rows Inserted {len(recovered_global.index)}')
    os.remove(file_path)
    logger.info(f'File {file_path} was deleted.')



process_file_CC_operator = PythonOperator(task_id='process_file_CC',
                                       dag=dag,
                                       python_callable = process_fileCC
                                       )

process_file_CD_operator = PythonOperator(task_id='process_file_CD',
                                       dag=dag,
                                       python_callable = process_fileCD
                                       )

process_file_CR_operator = PythonOperator(task_id='process_file_CR',
                                       dag=dag,
                                       python_callable = process_fileCR
                                       )



sensorCC >> process_file_CC_operator
sensorCD >> process_file_CD_operator
sensorCR >> process_file_CR_operator
