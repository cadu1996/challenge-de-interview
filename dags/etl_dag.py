from airflow import DAG
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id='azure_databricks_dag',
    start_date=days_ago(1),
    schedule_interval=None,
)

# Tasks to copy data to Azure Data Lake Gen 2
copy_to_azure_1 = LocalFilesystemToWasbOperator(
    task_id='copy_to_azure_1',
    file_path='/path/to/your/file1',
    container_name='azure_folder/file1',
    blob_name='file1',
    dag=dag,
)

copy_to_azure_2 = LocalFilesystemToWasbOperator(
    task_id='copy_to_azure_2',
    file_path='/path/to/your/file2',
    container_name='azure_folder/file2',
    blob_name='file2',
    dag=dag,
)

copy_to_azure_3 = LocalFilesystemToWasbOperator(
    task_id='copy_to_azure_3',
    file_path='/path/to/your/file3',
    container_name='azure_folder/file3',
    blob_name='file2',
    dag=dag,
)

# Tasks to run jobs on Databricks
run_databricks_job_1 = DatabricksSubmitRunOperator(
    task_id='run_databricks_job_1',
    databricks_conn_id='databricks_default',
    json={
      "new_cluster": {
        "spark_version": "7.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2
      },
      "notebook_task": {
        "notebook_path": "/path/to/your/notebook1",
      },
    },
    dag=dag,
)

run_databricks_job_2 = DatabricksSubmitRunOperator(
    task_id='run_databricks_job_2',
    databricks_conn_id='databricks_default',
    json={
      "new_cluster": {
        "spark_version": "7.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2
      },
      "notebook_task": {
        "notebook_path": "/path/to/your/notebook2",
      },
    },
    dag=dag,
)

run_databricks_job_3 = DatabricksSubmitRunOperator(
    task_id='run_databricks_job_3',
    databricks_conn_id='databricks_default',
    json={
      "new_cluster": {
        "spark_version": "7.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2
      },
      "notebook_task": {
        "notebook_path": "/path/to/your/notebook3",
      },
    },
    dag=dag,
)

# Tasks to run jobs on Databricks (again)
run_databricks_job_4 = DatabricksSubmitRunOperator(
    task_id='run_databricks_job_4',
    databricks_conn_id='databricks_default',
    json={
      "new_cluster": {
        "spark_version": "7.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2
      },
      "notebook_task": {
        "notebook_path": "/path/to/your/notebook4",
     

