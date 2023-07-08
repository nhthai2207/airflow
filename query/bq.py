from google.cloud import bigquery
from google.oauth2 import service_account

# Instantiate a BigQuery client
credentials = service_account.Credentials.from_service_account_file("/System/Volumes/Data/data/learning/airflow/big-cred.json")
client = bigquery.Client(credentials=credentials)
# client = bigquery.Client()

# Define your query
query = """
    SELECT * FROM `big-passage-391410.hello_world.hello_table` LIMIT 1000
"""

# Execute the query
query_job = client.query(query)

# Fetch the results
results = query_job.result()

# Process the results
for row in results:
    # Access individual columns by name
    column1_value = row["name"]
    column2_value = row["age"]
    
    # Do something with the data
    print(column1_value, column2_value)
