# Importing the required packages for all your data framing needs.
import pandas as pd

# The Snowflake Connector library.
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

## Phase I: Truncate/Delete the current data in the table
# The connector...
conn = snow.connect(user="abuton",
   password="Jimohm@ryam1",
   account="ez30284.us-central1.gcp",
   warehouse="compute_wh",
   database="competency",
   schema="trainees")

# Create a cursor object.
cur = conn.cursor()

# sql = "truncate table if exists YOUR_TABLE_NAME"
# cur.execute(sql)

# Close the cursor.
cur.close()

## Phase II: Upload from the Exported Data File.
# Let's import a new dataframe so that we can test this.
original = r"data/all_week_df_scaled.csv" 
delimiter = "," 

# Get it as a pandas dataframe.
df = pd.read_csv(original, sep = delimiter)

# Actually write to the table in snowflake.
write_pandas(conn, df, "COMPETENCY_SCORE")
print('data loaded to snowflake successfully')

# Create a cursor object.
cur = conn.cursor()

# Execute a statement that will turn the warehouse off.
sql = "ALTER WAREHOUSE compute_wh SUSPEND"
cur.execute(sql)

# Close your cursor and your connection.
cur.close()
conn.close()
