import pandas as pd
import pandas_gbq as pd_gbq
import urllib.request
from datetime import date
from bs4 import BeautifulSoup
import os

gcp_creds_path = '/Users/jason/Documents/Jason/JobMatch/job-match-271401-74d3c9eb9112.json'
if os.path.exists(gcp_creds_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/jason/Documents/Jason/JobMatch/job-match-271401-74d3c9eb9112.json'

print('here we go')

job_url_template = 'https://www.indeed.com/jobs?q={title}+%2420%2C000&l={location}&start={length}&sort=date'

job_titles = ['data+scientist', 'product+manager', 'data+analyst', 'full+stack+developer', 'frontend+developer', 'backend+developer', 'senior+developer',
              'ux+designer', 'ad+operations', 'human+resource', 'people+operations', 'technical+recruiter']
job_locations = ['New+York', 'Chicago', 'San+Francisco', 'Austin', 'Seattle',
                 'Los+Angeles', 'Philadelphia', 'Atlanta', 'Dallas', 'Pittsburgh',
                 'Portland', 'Phoenix', 'Denver', 'Houston', 'Miami',
                 'Charlottesville', 'Richmond', 'Baltimore', 'Harrisonburg', 'San+Antonio', 'San+Diego', 'San+Jose'
                 'Austin', 'Jacksonville', 'Indianapolis', 'Columbus', 'Fort+Worth', 'Charlotte', 'Detroit', 'El+Paso',
                 'Memphis', 'Boston', 'Nashville', 'Louisville', 'Milwaukee', 'Las+Vegas', 'Albuquerque', 'Tucson',
                 'Fresno', 'Sacramento', 'Long+Beach', 'Mesa', 'Virginia+Beach', 'Norfolk', 'Atlanta', 'Colorado+Springs',
                 'Raleigh', 'Omaha', 'Oakland', 'Tulsa', 'Minneapolis', 'Cleveland', 'Wichita', 'Arlington', 'New+Orleans',
                 'Bakersfield', 'Tampa', 'Honolulu', 'Anaheim', 'Aurora', 'Santa+Ana', 'Riverside', 'Corpus+Christi', 'Pittsburgh',
                 'Lexington', 'Anchorage', 'Cincinnati', 'Baton+Rouge', 'Chesapeake', 'Alexandria', 'Fairfax', 'Herndon',
                 'Reston', 'Roanoke']
result_length = 10


def url_to_df(job_url, search_title, search_location):
    soup = BeautifulSoup(urllib.request.urlopen(job_url).read(), 'html.parser')
    results = soup.find_all('div', attrs={'data-tn-component': 'organicJob'})

    output_df = pd.DataFrame()

    for x in results:
        input_date = date.today().strftime("%Y%m%d")
        input_search_title = search_title
        input_search_location = search_location
        input_company = None
        input_job_title = None
        input_location = None
        input_job_id = None
        # input_salary = None
        # input_job_url = None

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

        # salary = x.find('nobr')
        # if salary:
        #     input_salary = salary.text.strip()

        job_id = x.find('a').get('id')
        if job_id:
            input_job_id = job_id[3:]
            # input_job_url = 'https://www.indeed.com/viewjob?jk='+job_id[3:]

        job_df = pd.DataFrame(data=[{
            'search_date': input_date,
            'search_title': input_search_title,
            'search_location': input_search_location,
            'title': input_job_title,
            'location': input_location,
            'company': input_company,
            'job_id': input_job_id,
        }])

        output_df = output_df.append(job_df, ignore_index=True)
    return output_df


def search_jobs():
    jobs_df = pd.DataFrame()
    i = 0
    for job_title in job_titles:
        for job_location in job_locations:
            job_url = job_url_template.format(
                title=job_title, location=job_location, length=result_length)
            df = url_to_df(job_url, job_title, job_location)
            jobs_df = jobs_df.append(df, ignore_index=True)
            i += 1
            if i % 10 == 0:
                results_scaled = i*result_length
                print('Processed {num_results} number of results.'.format(
                    num_results=results_scaled))

    date_marker = str(date.today().strftime("%Y%m%d"))

    pd_gbq.to_gbq(jobs_df, 'job_search.job_search' +
                  date_marker, if_exists='replace')
    return print('Jobs Searched')


search_jobs()
