import pandas as pd
import pandas_gbq as pd_gbq
import urllib.request
from datetime import date
from bs4 import BeautifulSoup
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/jason/Documents/Jason/JobMatch/job-match-271401-74d3c9eb9112.json'

job_url_template = 'https://www.indeed.com/jobs?q={title}+%2420%2C000&l={location}&start={length}&sort=date'

job_titles = ['data+scientist', 'product+manager']
job_locations = ['New+York', 'Los+Angeles']
result_length = 20


def url_to_df(job_url):
    soup = BeautifulSoup(urllib.request.urlopen(job_url).read(), 'html.parser')
    results = soup.find_all('div', attrs={'data-tn-component': 'organicJob'})

    output_df = pd.DataFrame()

    for x in results:
        input_date = date.today().strftime("%Y%m%d")
        input_company = None
        input_job_title = None
        input_location = None
        input_job_id = None
        input_salary = None
        input_job_url = None

        company = x.find('span', attrs={"class": "company"})
        if company:
            input_company = company.text.strip()

        job_title = x.find('a', attrs={'data-tn-element': "jobTitle"})
        if job_title:
            input_job_title = job_title.text.strip()

        job_location = x.find(
            'span', {'class': "location"}).text.replace('\n', '')
        if job_location:
            input_location = job_location

        salary = x.find('nobr')
        if salary:
            input_salary = salary.text.strip()

        job_id = x.find('a').get('id')
        if job_id:
            input_job_id = job_id[3:]
            input_job_url = 'https://www.indeed.com/viewjob?jk='+job_id[3:]

        job_df = pd.DataFrame(data=[{
            'scraped_date': input_date,
            'title': input_job_title,
            'location': input_location,
            'company': input_company,
            'salary': input_salary,
            'job_id': input_job_id,
            'job_url': input_job_url
        }])

        output_df = output_df.append(job_df, ignore_index=True)
    return output_df


jobs_df = pd.DataFrame()
for job_title in job_titles:
    for job_location in job_locations:
        job_url = job_url_template.format(
            title=job_title, location=job_location, length=result_length)
        df = url_to_df(job_url)
        jobs_df = jobs_df.append(df, ignore_index=True)


date_marker = str(date.today().strftime("%Y%m%d"))

pd_gbq.to_gbq(jobs_df, 'job_search.job_search' +
              date_marker, if_exists='replace')
