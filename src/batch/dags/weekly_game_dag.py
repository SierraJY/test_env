import pendulum
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from scripts.kafka_weekly_producer import kafka_weekly_producer
from scripts.spark_weekly_game import spark_weekly_game

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weekly_game_dag',
    default_args=default_args,
    description='주간 경기 데이터 수집',
    schedule_interval='0 1 * * 1',  # 매주 월요일 오전 1시
    start_date=datetime(2022, 5, 1, tzinfo=local_tz),
    catchup=True,
    tags=['weekly', 'game', 'spark']
) as dag:

    # 카프카 프로듀셔
    # 기능 : 1주전 경기 데이터를 카프카 토픽에 전송
    produce_to_topic_operator = PythonOperator(
        task_id='produce_weekly_game_data',
        python_callable=kafka_weekly_producer,
        op_kwargs={
            'kafka_topic': 'weekly_game_data',
            'bootstrap_servers': 'kafka:9092',
            'execution_date': '{{ execution_date.strftime("%Y-%m-%d") }}'
        }
    )

    # 스파크 잡 제출
    # 기능 : 카프카 토픽에서 데이터를 수신하고 집계 및 피처 엔지니어링 후 데이터베이스에 저장
    submit_spark_job = SparkSubmitOperator(
        task_id='process_weekly_game_data',
        application='/opt/airflow/scripts/spark_weekly_game_processor.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.driver.memory': '1g',
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.python.worker.memory': '1g',
            'spark.ui.port': '4040'
        },
        application_args=[
            '--kafka_topic', 'weekly_game_data',
            '--bootstrap_servers', 'kafka:9092',
            '--execution_date', '{{ execution_date.strftime("%Y-%m-%d") }}',
            '--output_path', '/data/processed/weekly_games'
        ],
        verbose=True
    )

    # 작업 흐름 설정
    produce_to_topic_operator >> submit_spark_job