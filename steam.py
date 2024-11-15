from airflow import DAG
from datetime import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
import json
import os

current_path = os.getcwd()

dir_path = os.path.dirname(os.path.dirname(current_path))

with open(dir_path + '/steam/config.json', 'r') as arquivo:
  config = json.load(arquivo)

local_tz = pendulum.timezone('America/Sao_Paulo')

yesterday = pendulum.now(local_tz).subtract(days=1).date().strftime('%Y-%m-%d')

default_args = {
    "owner": config["user"],
    'email': config['email'],
    'email_on_retry': False,
    'email_on_failure': True,
    "start_date": datetime.strptime(yesterday, '%Y-%m-%d'),
}

dag = DAG(
    "steam",
    default_args=default_args,
    schedule_interval= "@hourly",
    catchup=False
)

# EXTRACT STEP

user1 = PapermillOperator(
    task_id='user_extract',
    input_nb=dir_path + "/projeto_steam/inbound/inbound_user.ipynb",
    dag=dag
)

wish1 = PapermillOperator(
    task_id='wish_extract',
    input_nb=dir_path + "/projeto_steam/inbound/inbound_wishlist.ipynb",
    output_nb='/dev/null',
    dag=dag
)

# BRONZE STEP

user2 = PapermillOperator(
    task_id='bronze_user',
    input_nb=dir_path + "/projeto_steam/bronze/user_bronze.ipynb",
    output_nb='/dev/null',
    dag=dag
)

wish2 = PapermillOperator(
    task_id='bronze_wish',
    input_nb=dir_path + "/projeto_steam/bronze/wish_bronze.ipynb",
    output_nb='/dev/null',
    dag=dag
)

# SILVER STEP

user3 = PapermillOperator(
    task_id='silver_user',
    input_nb=dir_path + "/rojeto_steam/silver/user_silver.ipynb",
    output_nb='/dev/null',
    dag=dag
)

wish3 = PapermillOperator(
    task_id='silver_wish',
    input_nb=dir_path + "/projeto_steam/silver/wish_silver.ipynb",
    output_nb='/dev/null',
    dag=dag
)

# GOLD STEP

user4 = PapermillOperator(
    task_id='gold_user',
    input_nb=dir_path + "/projeto_steam/gold/user_gold.ipynb",
    output_nb='/dev/null',
    dag=dag
)

wish4 = PapermillOperator(
    task_id='gold_wish',
    input_nb=dir_path + "/projeto_steam/gold/wish_gold.ipynb"
)

steam = PapermillOperator(
    task_id='union',
    input_nb=dir_path + "/projeto_steam/gold/gold_union.ipynb",
    output_nb='/dev/null',
    dag=dag
)


user1.set_upstream(list())  
wish1.set_upstream(list()) 
user2.set_upstream(list())  
wish2.set_upstream(list()) 
user3.set_upstream(list())  
wish3.set_upstream(list()) 
user4.set_upstream(list())  
wish4.set_upstream(list()) 

[user1, wish1]

user1 >> user2
wish1 >> wish2

[user2, wish2]

user2 >> user3
wish2 >> wish3

[user3, wish3]

user3 >> user4
wish3 >> wish4

[user4, wish4] >> steam