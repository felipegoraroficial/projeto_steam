from airflow import DAG
from datetime import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator

local_tz = pendulum.timezone('America/Sao_Paulo')

yesterday = pendulum.now(local_tz).subtract(days=1).date().strftime('%Y-%m-%d')

default_args = {
    "owner": ["fececa-dev"],
    'email': ['felipepegoraro93@gmail.com'],
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
    input_nb="/home/fececa/dev-env/projeto_steam/inbound/inbound_user.ipynb",
    output_nb='/home/fececa/dev-env/projeto_steam/inbound/inbound_user.ipynb',
    dag=dag
)

wish1 = PapermillOperator(
    task_id='wish_extract',
    input_nb="/home/fececa/dev-env/projeto_steam/inbound/inbound_wishlist.ipynb",
    output_nb='/home/fececa/dev-env/projeto_steam/inbound/inbound_wishlist.ipynb',
    dag=dag
)

# BRONZE STEP

user2 = PapermillOperator(
    task_id='bronze_user',
    input_nb="/home/fececa/dev-env/projeto_steam/bronze/user_bronze.ipynb",
    output_nb='/home/fececa/dev-env/projeto_steam/bronze/user_bronze.ipynb',
    dag=dag
)

wish2 = PapermillOperator(
    task_id='bronze_wish',
    input_nb="/home/fececa/dev-env/projeto_steam/bronze/wish_bronze.ipynb",
    output_nb='/home/fececa/dev-env/projeto_steam/bronze/wish_bronze.ipynb',
    dag=dag
)

# SILVER STEP

user3 = PapermillOperator(
    task_id='silver_user',
    input_nb="/home/fececa/dev-env/projeto_steam/silver/user_silver.ipynb",
    output_nb='/home/fececa/dev-env/projeto_steam/silver/user_silver.ipynb',
    dag=dag
)

wish3 = PapermillOperator(
    task_id='silver_wish',
    input_nb="/home/fececa/dev-env/projeto_steam/silver/wish_silver.ipynb",
    output_nb='/home/fececa/dev-env/projeto_steam/silver/wish_silver.ipynb',
    dag=dag
)

# GOLD STEP

user4 = PapermillOperator(
    task_id='gold_user',
    input_nb="/home/fececa/dev-env/projeto_steam/gold/user_gold.ipynb",
    output_nb='/home/fececa/dev-env/projeto_steam/gold/user_gold.ipynb',
    dag=dag
)

wish4 = PapermillOperator(
    task_id='gold_wish',
    input_nb="/home/fececa/dev-env/projeto_steam/gold/wish_gold.ipynb",
    output_nb='/home/fececa/dev-env/projeto_steam/gold/wish_gold.ipynb',
    dag=dag
)

steam = PapermillOperator(
    task_id='union',
    input_nb="/home/fececa/dev-env/projeto_steam/gold/gold_union.ipynb",
    output_nb='/home/fececa/dev-env/projeto_steam/gold/gold_union.ipynb',
    dag=dag
)

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
