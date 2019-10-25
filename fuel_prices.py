import requests as re
import psycopg2 as con
import pandas as pd
from bs4 import BeautifulSoup

url = 'https://economictimes.indiatimes.com/wealth/fuel-price'
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'}
response = re.get(url,headers=headers)

tables = pd.read_html(response.text)
city_data = tables[0]
state_data = tables[1]

#Modifing the data
city_columms = ['city','fuel_price','date']
state_columns = ['state','fuel_price','date']

#city data
city_data.drop('CHANGE',axis=1,inplace=True)
city_data['Fuel Price'] = city_data['Fuel Price'].apply(lambda x: x.replace('₹/L',''))
city_data['Fuel Price'] = city_data['Fuel Price'].astype('float')
city_data['date'] = pd.datetime.now().date()
city_data.columns = city_columms

# state_data
state_data.drop('CHANGE',axis=1,inplace=True)
state_data['Fuel Price'] = state_data['Fuel Price'].apply(lambda x: x.replace('₹/L',''))
state_data['Fuel Price'] = state_data['Fuel Price'].astype('float')
state_data['date'] = pd.datetime.now().date()
state_data.columns = state_columns


print(city_data.head())
print('\n\n\n\n\n\n\n')
print(state_data.head())