import multiprocessing
import pandas as pd
import urllib.request
from datetime import datetime
from bs4 import BeautifulSoup
import os

gcp_creds_path = '/Users/jason/Documents/Jason/JobMatch/job-match-271401-74d3c9eb9112.json'
if os.path.exists(gcp_creds_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/jason/Documents/Jason/JobMatch/job-match-271401-74d3c9eb9112.json'


def retrieve_job_id():
    query_string = """
    select *, ABS(MOD(FARM_FINGERPRINT(job_id), 10)) part
    from job_search.job_id_new_list
    """
    df = pd.read_gbq(query=query_string, progress_bar_type=None)
    return df


def job_details(job_id, title, location, company):
    input_datetime = datetime.today().strftime("%Y%m%d %H:%M")
    job_desc_url = 'https://www.indeed.com/viewjob?jk='+job_id
    soup = BeautifulSoup(urllib.request.urlopen(
        job_desc_url).read(), 'html.parser')

    try:
        job_title = soup.find(
            'div', attrs={"class": "jobsearch-JobInfoHeader-title-container"})
        job_title = job_title.text.strip()
    except:
        job_title = title

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
        company_name = company

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
        'title': job_title,
        'job_desc': job_desc,
        'location': location,
        'company': company_name,
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


def search_job_desc(input_df):
    df = input_df.reset_index(drop=True)
    df_job_details = pd.DataFrame()
    process_id = df.job_id.iloc[0]
    print('Starting Parallel Process ' + str(process_id))
    for index, row in df.iterrows():
        row_job_details = job_details(row['job_id'], row['title'],
                                      row['location'], row['company'])
        df_job_details = df_job_details.append(
            row_job_details, ignore_index=True)
        if (index+1) % 100 == 0:
            print(
                process_id + ' -- Uploading to BigQuery. Job Desc Count at = ' + str(index+1))
            df_to_bq(df_job_details)
            df_job_details = pd.DataFrame()
    df_to_bq(df_job_details)
    return print('job done!')


if __name__ == '__main__':
    df = retrieve_job_id()
    pool = multiprocessing.Pool(processes=6)
    inputs = [df[df.part == 0], df[df.part == 1],
              df[df.part == 2], df[df.part == 3], df[df.part == 4], df[df.part == 5], df[df.part == 6], df[df.part == 7], df[df.part == 8], df[df.part == 9]]
    outputs = pool.map(search_job_desc, inputs)
