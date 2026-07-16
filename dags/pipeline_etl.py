from datetime import datetime
from pathlib import Path

from src.etl import ejecutar_pipeline


def dag_pipeline_etl():
    """Ejecuta el pipeline ETL completo desde Airflow."""
    base_dir = Path(__file__).resolve().parents[1]
    return ejecutar_pipeline(base_dir=base_dir, exportar_a_postgres=False)


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="pipeline_etl",
        description="Ejecuta el pipeline ETL para cargar, limpiar y preparar los datos",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["etl", "analisis"],
    ) as dag:
        ejecutar_tarea = PythonOperator(
            task_id="ejecutar_pipeline_etl",
            python_callable=dag_pipeline_etl,
        )
except ImportError:
    DAG = None
    PythonOperator = None
    dag = None
