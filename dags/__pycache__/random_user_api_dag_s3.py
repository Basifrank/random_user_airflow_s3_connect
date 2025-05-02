
import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import awswrangler as wr
import pandas as pd
import requests
import boto3
from airflow.models import Variable
from dotenv import load_dotenv
load_dotenv()

#from random_user import extract_api_data, transform_male_df, load_male_to_s3

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

def extract_api_data():
    """
    fundtion to get data from the API
    Arg: Link to the API
    """
    api_content = requests.get("https://randomuser.me/api/?results=10")
    if api_content.status_code == 200:
        data = api_content.json()
    else:
        print("Error fetching data from API")
        return None
    return data
    

def transform_male_df():
    raw_data = extract_api_data()
    male = []
    for profile in raw_data["results"]:
        if profile["gender"] == "male":
            male.append(profile["gender"])

    male = pd.DataFrame(male, columns=["male_gender"])
    return male
    

def load_male_to_s3():
    df = transform_male_df()
    load_dotenv()
        
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name=Variable.get("AWS_REGION")
        )
        
    raw_s3_bucket = "chigozieobasi"
    raw_path_dir = "randomuser_api_airflow_from_docker"
    csv_path = "updated_personaldata"
    path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{csv_path}"

    wr.s3.to_parquet(df=df, 
                         path=path + "/male", 
                         dataset=True, 
                         mode="append",
                         boto3_session=session)





default_args = {
  
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=5)
    
}

dag = DAG(
    dag_id='randomuser_api_dag_s3',
    default_args=default_args,
    description='A DAG to send data to S3',
)

#Define the tasks

extract_data = PythonOperator(
        dag=dag,
        python_callable=extract_api_data,
        task_id='get_api_data'
        
    )


extract_male_data = PythonOperator(
        dag=dag,
        python_callable=transform_male_df,
        task_id='get_male_data'
        
    )

write_male_s3data = PythonOperator(
        dag=dag,
        python_callable=load_male_to_s3,
        task_id='write_male_data'
        
    )


extract_data >> extract_male_data >> write_male_s3data

#extract_data.set_downstream([extract_male_data, write_male_s3data])