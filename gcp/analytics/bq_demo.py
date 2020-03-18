from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/jason/Documents/Jason/JobMatch/job-match-271401-74d3c9eb9112.json'

query = """
select *
from job_search.job_search20200317
"""

bq_client = bigquery.Client()

df = bq_client.query(query).result().to_dataframe()
