import pandas as pd
import urllib.request
from datetime import datetime
from bs4 import BeautifulSoup
import os

# https://www.confessionsofadataguy.com/web-scraping-sentiment-spark-streaming-postgres-dooms-day-clock-part-1/
# https://compiletoi.net/fast-scraping-in-python-with-asyncio/
import asyncio
from aiohttp import ClientSession

gcp_creds_path = '/Users/jason/Documents/Jason/JobMatch/job-match-271401-74d3c9eb9112.json'
if os.path.exists(gcp_creds_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/jason/Documents/Jason/JobMatch/job-match-271401-74d3c9eb9112.json'


def retrieve_job_id():
    query_string = """
    select job_id, new_search_datetime as search_datetime,
        title, location, company, search_title, search_location
    from job_search.job_id_new_list
    """
    df = pd.read_gbq(query=query_string, progress_bar_type=None)
    return df


### Async Web Scraping ###

# Set the number of concurrent requests to limit hits to Indeed webserver
sema = asyncio.BoundedSemaphore(10)


async def fetch(url, session):
    async with sema, session.get(url) as response:
        return await response.read()


async def run_urls(job_id_list):
    tasks = []

    async with ClientSession() as session:
        for job_id in job_id_list:
            job_url = 'https://www.indeed.com/viewjob?jk='+job_id
            task = asyncio.ensure_future(fetch(job_url, session))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        return responses

# Responses to BeautifulSoup


def job_details(response):
    input_datetime = datetime.today().strftime("%Y%m%d %H:%M")
    soup = BeautifulSoup(response, 'html.parser')

    try:  # Finds job_id from response through meta tag
        job_id = soup.find('meta', attrs={'id': 'indeed-share-url'})
        job_id = job_id['content'].split('=')[-1]
    except:
        job_id = None

    try:
        job_title = soup.find(
            'div', attrs={"class": "jobsearch-JobInfoHeader-title-container"})
        job_title = job_title.text.strip()
    except:
        job_title = None

    try:
        job_desc = soup.find(
            'div', attrs={'class': 'jobsearch-jobDescriptionText'})
    except:
        job_desc = None

    try:
        company_name = soup.find(
            'a', attrs={"class": "jobsearch-CompanyAvatar-companyLink"})
        company_name = company_name.text.strip()
    except:
        company_name = None

    try:
        company_rating = soup.find('meta', attrs={'itemprop': 'ratingValue'})
        company_rating = company_rating['content']
    except:
        company_rating = 0

    try:
        company_rating_count = soup.find(
            'meta', attrs={'itemprop': 'ratingCount'})
        company_rating_count = company_rating_count['content']
    except:
        company_rating_count = 0

    df_job_details = pd.DataFrame(data=[{
        'search_detail_datetime': input_datetime,
        'job_id': job_id,
        'desc_title': job_title,
        'job_desc': job_desc,
        'desc_company': company_name,
        'company_rating': company_rating,
        'company_rating_count': company_rating_count,

    }])
    return df_job_details


def df_to_bq(input_df):
    # custom function to bulk convert datatypes
    input_df.search_detail_datetime = pd.to_datetime(
        input_df.search_detail_datetime, format="%Y%m%d %H:%M")
    input_df.company_rating = input_df.company_rating.astype(float)
    input_df.company_rating_count = input_df.company_rating_count.astype(int)
    pd.io.gbq.to_gbq(input_df, 'job_search.job_details',
                     'job-match-271401', if_exists='append', progress_bar=False)
    return


def responses_to_df(responses):
    df_job_details = pd.DataFrame()
    for response in responses:
        row_job_details = job_details(response)
        df_job_details = df_job_details.append(
            row_job_details, ignore_index=True)
    return df_job_details


def divide_list(input_list, n):
    for i in range(0, len(input_list), n):
        yield input_list[i:i+n]


def job_desc(request):
    request = 'not used'
    df_job_search = retrieve_job_id()  # get job_ids
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run_urls(df_job_search.job_id.to_list()))
    responses = loop.run_until_complete(future)
    # Split into batch sizes of 400
    list_of_responses = list(divide_list(responses, 400))
    for i, i_response in enumerate(list_of_responses):
        df_job_details = responses_to_df(i_response)
        df_combined = pd.merge(df_job_search, df_job_details,
                               how='inner', left_on='job_id', right_on='job_id')
        df_combined.desc_title.fillna(df_combined.title, inplace=True)
        df_combined.desc_company.fillna(df_combined.company, inplace=True)
        df_combined.drop(columns=['title', 'company'], inplace=True)
        df_combined.rename(
            columns={"desc_title": "title", "desc_company": "company"}, inplace=True)
        df_to_bq(df_combined)
        print('Processed and Uploaded: ' + str((i+1)*400) + ' Job Descriptions')
    return
