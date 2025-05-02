import awswrangler as wr
import pandas as pd
import requests
import boto3
from airflow.models import Variable
from dotenv import load_dotenv
load_dotenv()





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
    csv_path = "personaldata"
    path = f"s3://{raw_s3_bucket}/{raw_path_dir}/{csv_path}"

    wr.s3.to_parquet(df=df, 
                         path=path + "/male", 
                         dataset=True, 
                         mode="append",
                         boto3_session=session)


