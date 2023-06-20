from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

default_args = {"owner": "Carlos Eduardo de Souza"}


with DAG(
    "ETL_log_failures",
    start_date=days_ago(1),
    schedule_interval=None,
    default_args=default_args,
) as dag:
    with TaskGroup("staging_to_raw") as staging_to_raw:
        equipment_failure_sensor = DatabricksRunNowOperator(
            task_id="equipment_failure_sensor",
            databricks_conn_id="databricks_default",
            job_id=360232576843179,
        )

        equipment_sensor = DatabricksRunNowOperator(
            task_id="equipment_sensor",
            databricks_conn_id="databricks_default",
            job_id=1101807116395376,
        )

        equipment = DatabricksRunNowOperator(
            task_id="equipment",
            databricks_conn_id="databricks_default",
            job_id=909156579247786,
        )

    failure_log_table = DatabricksRunNowOperator(
        task_id="failure_log_table",
        databricks_conn_id="databricks_default",
        job_id=520504070910103,
    )

    log_failure_analysis = DatabricksRunNowOperator(
        task_id="log_failure_analysis",
        databricks_conn_id="databricks_default",
        job_id=1091197516623674,
    )

    staging_to_raw >> failure_log_table >> log_failure_analysis
