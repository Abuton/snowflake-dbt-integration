# Airflow Dag
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from snowflake.connector import connection

USER = 'abuton'
DB = 'competency'
TABLE_NAME = 'competency_score'
SCHEMA = 'trainees'
PATH = r'/home/Abuton/Desktop/ML_PATH/week0/dwh-techstack/sensorCompany/data/all_week_df_scaled.csv'

# Importing the required packages for all your data framing needs.
import pandas as pd
# The Snowflake Connector library.
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

default_args = {
    'owner': 'Abubakar Alaro',
    'start_date': days_ago(1),
    'depends_on_past': False,
   #  'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(1), tags=['snowflake'])
def snowflake_loader():
   """
   
   """
   @task()
   def create_snowflake_connection():
      ## Phase I: Truncate/Delete the current data in the table
      # The connector...
      conn = snow.connect(user=USER,
         password="Jimohm@ryam1",
         account="ez30284.us-central1.gcp",
         warehouse="compute_wh",
         database=DB,
         schema=SCHEMA)

      # Create a cursor object.
      cur = conn.cursor()
      print('Cursor Object created Successfully')

      sql = f"truncate table if exists {TABLE_NAME}"
      cur.execute(sql)
      print(f'{TABLE_NAME} dropped')

      # Close the cursor.
      cur.close()
      return conn

   @task()
   def load_dataframe(path):

      ## Phase II: Upload from the Exported Data File.
      # Let's import a new dataframe so that we can test this.
      original = path 
      delimiter = "," 

      # Get it as a pandas dataframe.
      df = pd.read_csv(original, sep = delimiter)
      if len(df) > 0:
         print('DataFrame Loaded Successfully')
         return df

   @task()
   def load_data_to_snowflake(conn:str, data:pd.DataFrame):

      # Actually write to the table in snowflake.
      write_pandas(conn, data, TABLE_NAME)
      print('data loaded to snowflake successfully')

      # Create a cursor object.
      cur = conn.cursor()

      # Execute a statement that will turn the warehouse off.
      sql = "ALTER WAREHOUSE compute_wh SUSPEND"
      cur.execute(sql)
      print('warehouse closed!')

      # Close your cursor and your connection.
      cur.close()
      conn.close()
      print('connection closed')

   connection = create_snowflake_connection()
   df = load_dataframe(path=PATH)
   load_data_to_snowflake(conn=connection, data=df)

snowflake_data_loader = snowflake_loader()
