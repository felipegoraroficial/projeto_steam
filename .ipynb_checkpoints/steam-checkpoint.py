from airflow import DAG
from datetime import datetime
import pendulum
from airflow.operators.python import PythonOperator
from steam.extract.extract_user import extract_user_steam
from steam.bronze.user_bronze import bronze_user_steam
from steam.silver.user_silver import silver_user_steam
from steam.extract.extract_wishlist import extract_wishlist_steam
from steam.bronze.wish_bronze import bronze_wishlist_steam
from steam.silver.wish_silver import silver_wish_steam
from steam.gold.user_gold import gold_user_steam
from steam.gold.wish_gold import gold_wish_steam
from steam.gold.gold_union import union_gold



# Defina o fuso horário desejado (São Paulo, Brasil)
local_tz = pendulum.timezone('America/Sao_Paulo')

default_args = {
    "owner": "felipe.pegoraro",
    'email': ['felipepegoraro93@gmail.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    "start_date": datetime(2024, 6, 24, tzinfo=local_tz),
}

dag = DAG(
    "steam",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False
)

# EXTRACT STEP

user1 = PythonOperator(
    task_id='user_extract',
    python_callable=extract_user_steam,
    dag=dag
)

wish1 = PythonOperator(
    task_id='wish_extract',
    python_callable=extract_wishlist_steam,
    dag=dag
)

# BRONZE STEP

user2 = PythonOperator(
    task_id='bronze_user',
    python_callable=bronze_user_steam,
    dag=dag
)

wish2 = PythonOperator(
    task_id='bronze_wish',
    python_callable=bronze_wishlist_steam,
    dag=dag
)

# SILVER STEP

user3 = PythonOperator(
    task_id='silver_user',
    python_callable=silver_user_steam,
    dag=dag
)

wish3 = PythonOperator(
    task_id='silver_wish',
    python_callable=silver_wish_steam,
    dag=dag
)

# GOLD STEP

user4 = PythonOperator(
    task_id='gold_user',
    python_callable=gold_user_steam,
    dag=dag
)

wish4 = PythonOperator(
    task_id='gold_wish',
    python_callable=gold_wish_steam,
    dag=dag
)

steam = PythonOperator(
    task_id='union',
    python_callable=union_gold,
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