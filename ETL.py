import requests as r
import random
from datetime import (date, datetime, timedelta)
import re
import json
import pandas as pd
import time as t
import gspread
from google.oauth2.service_account import Credentials
#transform Data
def transform(Data,data_dict):
    for data in Data:
        product = data["_source"]["product"]
        issue = data["_source"]["issue"]
        sub_product = data["_source"]["sub_product"]
        complaint_id = data["_source"]["complaint_id"]
        timely = data["_source"]["timely"]
        company_response = data["_source"]["company_response"]
        submitted_via = data["_source"]["submitted_via"]
        company = data["_source"]["company"]
        state = data["_source"]["state"]
        sub_issue = data["_source"]["sub_issue"]
        if sub_issue is None:
            sub_issue = 'None'
        else:
            sub_issue
        _date = data["_source"]["date_received"]
        match = re.search(r'\d{4}-\d{2}-\d{2}', _date)
        res = datetime.strptime(match.group(), '%Y-%m-%d').date()
        date_received = datetime.strftime(res, '%b-%Y')

        data_dict.get('complaint_id').append(complaint_id)
        data_dict.get('product').append(product)
        data_dict.get('sub_product').append(sub_product)
        data_dict.get('issue').append(issue)
        data_dict.get('sub_issue').append(sub_issue)
        data_dict.get('submitted_via').append(submitted_via)
        data_dict.get('company').append(company)
        data_dict.get('timely').append(timely)
        data_dict.get('company_response').append(company_response)
        data_dict.get('date_received').append(date_received)
        data_dict.get('state').append(state)
    return data_dict

#extract for dag
def _extract_data(ti):
    
    states = list(r.get("https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json").json().keys())
    base_url = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/'

    headers = {
        'authority': 'www.consumerfinance.gov',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        # Requests sorts cookies= alphabetically
        # 'cookie': f"_gid=GA1.2.1120086265.1661842548; mf_user=4b8b9d7b9636d7d9a9b353bf86fb9325|; _ga=GA1.2.326601816.1653886593; _ga_CMRC03R7CT=GS1.1.1661876905.5.1.1661877160.0.0.0; _ga_8G78BL5ZLY=GS1.1.1661876905.5.1.1661877160.0.0.0; _ga_48CT2JKQ6K=GS1.1.1661876905.5.1.1661877160.0.0.0; _ga_DBYJL30CHS=GS1.1.1661876905.5.1.1661877160.0.0.0; _gat_UA-54439736-1=1; mf_7b540e59-4067-4ee2-b344-b56ac0d44801=eb05b59aab6b6632adcc917313c37468|08304205e468e387f00c5ecef2edc575bba6f0d0.-1165385897.1661858202906{08303057de8fcf20ad83c060d46b4bf27eff4bf5.-1789404624.1661876970659$08304405e7c06616b29f2b7c0f4341f525f3138b.-1165385897.1661876984308$0830435798bc9f038ae39d0b91c5e582a7e3e8a4.47.1661877103760$0830038381a6123b1390632132033d3635e48f61.-1789404624.1661877123884$083041677a5fdd416b76c5c4d6a5f09739379152.-1165385897.1661877161771|1661877230072||7|||0|17.70|2.9628}",
        'referer': 'https://www.consumerfinance.gov/data-research/consumer-complaints/',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
         'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    }
    s = r.session()
        
    p = {
        'field': 'complaint_what_happened',
        'size': [],
        'date_received_max': [],
        'date_received_min': [],
        'state': [],
    }
    #give data of one year
    time_delta = 365

    #defining parameters
    p['size'] = 5000
    p['date_received_max'] = date(2022, 7, 30).strftime("%Y-%m-%d")
    p['date_received_min'] = (date(2022, 7, 30) - timedelta(days=time_delta)).strftime("%Y-%m-%d")
    extracted_data_list = []
    number_of_states = 20
    #randomly choose 8 states from the list of states and append it to extracted_data_list
    for state in random.choices(states,k=number_of_states):
        p['state'] = state
        print(state)
        results = s.get(url=base_url, params=p, headers=headers).json()
        extracted_data_list.append(results)
        t.sleep(3)

    #convert list into dict so it can be push and pulled by xcom
    push_json= {'r':extracted_data_list}
    
    ti.xcom_push(key='api_data',value=push_json)
    return 'Extraction Successful'

#transform for dag
def _transform_data(ti):
    result = ti.xcom_pull(key='api_data',task_ids='extract_data')
    
    required_data = result['r']   
    dictionary = {
        "complaint_id": [],
        "product": [],
        "sub_product": [],
        "issue": [],
        "sub_issue":[],    
        "submitted_via":[],    
        "company":[],
        "state":[],
        "timely":[],
        "company_response":[],     
        "date_received":[]  
    }
    #place the extracted data in loop and use try and except since some states doesn't have data
    for tag in required_data:
        try:
            extracted_data=tag['hits']['hits']
            transformed_data = transform(extracted_data, dictionary)
            print("Getting Data")
        except:
            print('Skipped')
            pass
    df = pd.DataFrame(transformed_data)
    df2 = df.groupby(df.columns.tolist()).size().reset_index(name='No of Complaints')
    push_df_json= df2.to_dict()

    ti.xcom_push(key='cleaned_data',value=push_df_json)
    return "Transformation Successful"

#load for dag
def _load_data(ti):
    tf_data = ti.xcom_pull(key='cleaned_data',task_ids='transform_data')       
    _df = pd.DataFrame(tf_data)

    columns = _df.columns.tolist() #give all the column names
    values = _df.values.tolist() #give all the data beside the column names
    values.insert(0, columns) #append columns to the values

    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    creds_json ={
        "type": "service_account",
        "project_id": "scrappeddata",
        "private_key_id": "951cacfe5b7f497378542cea285f13358cb60b13",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCX+nhdw5Lhocg5\nH3/szspAiKFxpwua95TCsgKJYAKxFkAoZlqVmDz9G8fAtaeJ3mhjMHeQN9TYKo5Z\nIACZjevq4e3YbYeV3l9ZfZoc/uz39KSJ//1lB83Uic3we9P84gHYcH1zTEdFsvAA\n8FfVUYum6L5ElM7mkvudgpa9b0H338eys+i1sNxAmYcWRYJcX77qya72Rpsqy2Q+\nyQjlPyfe+/Sd59D7CQQO4dcM8bN1wx3H38MvcaDcZlEhHa3moV53eIbmj/NCtUyH\nkZh0u8oAAfhDSbltLLXfAFIJT1Lb2XqPn6jg6CEi2eotNG6vD31wknhWl6wTHU2E\nBdG31DP3AgMBAAECggEABZWO2BuWfqh13MPoeuoZlSfI8fGDMkONYJeS2i0HOLxt\n89i71JE3SkAC5co1KK7BONQ+WjPz3MW4aylglu57NytBP2tLwbHsGYkw4TFQFysv\nVTaJeBEmEEOrxDadBA3iirJVNwub9wh9TacVWVbycBLdjRsANaXmm7APot6bgWur\nOuaeBl/xZgpZr0NMGUzYqxxR4rjbzEjnSaF90y5WOhO4pnRJdU8CZfFF+xuElMGm\nB5POQAWBSWFkvdtWGFNzlZ8SzmVeUCFa6dvTY6+L8JkL7baB2QTRfUWmdCVPsKW8\nNafqi8S1IWeekmQg7QZ7pzhTyeMpmBOVj0w7CZ7kHQKBgQDG96XClPysdNu6c4Q6\nVzRMTwrP/NLc7xDOTlmj7NF5062si6VFGH0LCdf0xCWSl1iNW0czwXVZ4J+MP211\nOluaPR/QiJN1qm0JzCbaJCrMgC1D+vkz3iKCGQ2Jdz94ZYV8Jy2SvpgZFNOU8HqN\n97KQAi4Gh8rimrhfvMpCKmKdwwKBgQDDir3qoWm9ofb21y3zWg1Y/gY9oIcKx2SF\nKwqO7sK7S3gjNdzaRz5MHKIKBwJqOpeOU7+u0759hVQAInDD83u9dq2k11CDIQau\nOVtcdhCqtDy2usOPa07ZFlVTkBWxue/yowtgJz8uJ6kB2QuSnwicVilk+YMdwqmJ\nju8x06CpvQKBgQC7/Ua3l4843lyxSO25NaWrJ89+flE5AAPv+SVG9b1iDvd8HKE1\nrJKQuGc704vwemDnhkO7CeF33vzARTmFVnMwQuppXHF+7lyBsktPNnbdq4mZPmrw\nb8RJCaS5qlrkxX8es+GiUcRhhkT251PUCSHEYNrdvrWZ7zee9UBgza1JgwKBgQCm\nEzb7iL3TBjVHLjhjn48Ijy7jtmwbNn/kLksYXCZdhlpxpTR+2tVJNWRWGd/uMvc3\nNV/ubv4xg0R1nMqjUi89GgeeohRKWole/W3f2JKNOOj71SF0tJHSBIrRnuUd/iqR\nq/JkZtDdNWfwHtQaYiGhDfi4PsYiR4tQPmpeqjuhDQKBgBbVxd6H8mgvx31fWgQ3\nuwlNPSj6I+6tY8sQczbTUFcY8U30Lpt2MHq0+hQqdVfufnHinH6C70rIeMcjFN/e\nLz8jmF74hOYaWut4pguGwgQOi9Q1B1cAaBvaojJOH4Qp1IjwzjUa51LJgDMSXiEi\nzNjFCRPqGU10MgXkKkA+d5Ol\n-----END PRIVATE KEY-----\n",
        "client_email": "gsheetscrapper@scrappeddata.iam.gserviceaccount.com",
        "client_id": "109482424505602571700",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gsheetscrapper%40scrappeddata.iam.gserviceaccount.com"
        }
    credentials = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(credentials)
    gs = gc.open('CFPB_Dataset')
    worksheet = 'Data'
    
    #if there is some data in the sheet, it will be cleared and will replace the data with the current data, else it will add the data directly 
    if not gs.values_batch_get(worksheet):
            gs.values_append(worksheet, {'valueInputOption': 'RAW'}, {'values': values})
    else:
        gs.values_clear(worksheet)
        gs.values_append(worksheet, {'valueInputOption': 'RAW'}, {'values': values})
    return 'Loading Successful'

#link to the googlesheet for the dumped data 
# https://docs.google.com/spreadsheets/d/1tRCyUldCZId_UTkXzjjhJvDNPjQfUSEFskpfHiXDESk/edit#gid=0