import os
from dotenv import load_dotenv
import pandas as pd
import re
from pdfminer.high_level import extract_text, LAParams

from pathlib import Path

load_dotenv()  # take environment variables from .env.
key = os.environ.get("API_KEY")

import requests
from urllib.parse import quote

from tqdm import tqdm

def loc(street):
    addy = street
    
    addy = quote(addy)
    response = requests.get(f'https://maps.googleapis.com/maps/api/geocode/json?address={addy}&key={key}')
    resp_json_payload = response.json()
    # print(resp_json_payload)
    return resp_json_payload['results'][0]['geometry']['location']['lat'], resp_json_payload['results'][0]['geometry']['location']['lng']

def save_pdf_to_csv(pdf_loc, name, save_loc):
    text = extract_text(pdf_loc, laparams = LAParams(boxes_flow=None))
    
    ts = text.split('\n')
    ts = [i for i in ts if i != '']

    f = open(f'cleaned_txt/clean_{name}.txt', 'w')
    for i in ts:
        # remove hex characters
        line = re.sub(r'[^\x00-\x7f]',r'', i) 

        # heuristical skipping
        if i in ['TOMPKINS COUNTY SHERIFFS','OFFICE', 'Public Information Log', 'Public Information Log']:
            continue
        if i[:4] == 'Page':
            continue
        if i[0].isdigit():
            continue
                
        print(line, file=f)

    f.close()

    reasons = []
    incident_address = []
    time_reported = []
    time_occured = []
    comments = []

    if_last_is_incident = False
    if_last_time_occured = True

    tot = ""
    prev = ""
    with open(f'cleaned_txt/clean_{name}.txt', 'r') as f:
        for ind, line in enumerate(f.readlines()):
            line = line.strip()
            
            first_colon = line.find(':')

            # heuristic adding to 
            if first_colon != -1:
                before = line[:first_colon]
                after = line[first_colon + 1:]
                if before == 'Incident Address':
                    
                    # append previous comments if not start as well as next reason
                    if ind != 1:
                        comments.append(tot)
                    reasons.append(prev)
                    tot = ""
                    prev = ""
                    if_last_time_occured = False


                    incident_address.append(after)

                    if_last_is_incident = True

                elif before == 'Time Reported':
                    if_last_is_incident = False
                    time_reported.append(after)
                elif before == 'Time Occurred Between':
                    time_occured.append(after)
                    if_last_time_occured = True
            else:
                if if_last_is_incident:
                    # Add to address
                    incident_address[-1] += ' ' + line
                elif if_last_time_occured:
                    tot += " " + prev
                    prev = line
                # if first line
                elif ind == 0:
                    prev = line

                else:
                    pass
        comments.append(tot)
    len(incident_address), len(time_reported), len(time_occured), len(reasons), len(comments)

    lat = []
    long = []


    df = pd.DataFrame.from_dict({
        'reasons' : reasons,
        'time_occured' : time_occured,
        'time_reported' : time_reported,
        'comments' : comments,
        'incident_address' : incident_address,
    })
    # get only traffic data
    df = df[df['reasons'].str.contains('Traffic')]

    for i in tqdm(df['incident_address'], desc='Getting locations'):
        s = i.split(';')
        if len(s) > 1:
            c = s[0] + ' '+ s[-1][-5:]
        else:
            c = s[0] + ' Tompkins County NY'

        try:
            location = loc(c)
            lat.append(location[0])
            long.append(location[1])
        except:
            lat.append("")
            long.append("")
    df['lat'] = lat
    df['long'] = long

    df = df[df['lat'] != '']

    df.to_csv(save_loc)
            

months_d = {'January': 31, 'February': 29, 'March': 31, 'April': 30, 'May': 31, 'June': 30, 'July': 31, 'August': 31, 'September': 30, 'October': 31, 'November': 30, 'December': 31}

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
          'August', 'September', 'October', 'November', 'December']
def scrape_sheriff_logs():
    for i in range(2021, 2023):
        try:
            for m in tqdm(months):
                link = f'https://tompkinscountyny.gov/files2/files2/sheriff/Daily-Logs/{m}%201-{months_d[m]}%2C%2020{i % 1000}.pdf'

                response = requests.get(link)
                with open(f'uncleaned_files/{m}_{i}.pdf', 'wb') as f:
                    f.write(response.content)
        except:
            pass



def clean_pdfs():
    for f in tqdm(os.listdir('uncleaned_files/'), desc='Iterating thru months'):
        try:
            if not Path(f'scraped_months/{f[:-4]}').exists():
                save_pdf_to_csv(f'uncleaned_files/{f}', f[:-4], f'scraped_months/{f[:-4]}')
        except:
            print(f)


def save_to_one_csv():
    headers = ['reasons', 'time_occured', 'time_reported', 'comments', 'incident_address', 'lat', 'long']

    pd.DataFrame(columns=headers).to_csv('whole.csv', index=False)

    for f in tqdm(os.listdir('scraped_months/'), desc='Iterating thru months'):

        print(f)
        x = pd.read_csv(f'scraped_months/{f}', index_col=0)


        print(x.columns)

        x.to_csv('whole.csv', mode='a', header=False, index=False)


def process_time_started():
    x = pd.read_csv('whole.csv')
    x[['time_started', 'time_ended']] = x['time_occured'].str.split('-', 1, expand=True)

    x['time_started'] = pd.to_datetime(x['time_started'])
    x['time_ended'] = pd.to_datetime(x['time_ended'], errors='coerce')

    x.to_csv('whole_with_time.csv')


def accidents_to_csv():
    accident_df = pd.read_csv('accidents_cleaned.csv')

    lats = []
    longs= []
    with tqdm(total=len(accident_df)) as pbar:
        for ind, row in tqdm(accident_df.iterrows()):
        
            loc_to_search = ""
            if pd.isnull(row['intersection']):
                loc_to_search = row['road']
            else:
                loc_to_search = row['intersection']
            
            loc_to_search += ', Tompkins County NY'

            lat, long = loc(loc_to_search)

            lats.append(lat)
            longs.append(long)
            pbar.update(1)

    accident_df['lat'] = lats
    accident_df['long'] = longs

    accident_df.to_csv('accident_with_locations.csv')

x = pd.read_csv('accident_with_locations.csv')
x.drop(x.columns[[0, 1]], axis=1, inplace=True)
# print(x.head())

x['seconds_since'] = pd.to_datetime(x['Time']).dt.hour * 3600 + pd.to_datetime(x['Time']).dt.minute * 60 + pd.to_datetime(x['Time']).dt.second

x['month'] = pd.to_datetime(x['Prd Date']).dt.month


x.to_csv('accidents_seconds_since.csv', index=False)