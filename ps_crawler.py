import requests
from bs4 import BeautifulSoup as bs
import time
from tqdm import tqdm
import sqlite3
import pandas as pd
import itertools
import  cProfile
import lxml
import grequests


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
url = r'https://store.playstation.com/en-us/category/dc464929-edee-48a5-bcd3-1e6f5250ae80'

def get_urls():
    urls = []
    
    r= requests.get(url,headers = headers)
    soup = bs(r.content,'html.parser')
    value = soup.find_all(class_='psw-button psw-b-0 psw-page-button psw-p-x-3 psw-r-pill psw-l-line-center psw-l-inline psw-t-size-3 psw-t-align-c')[-1].text
    
    for i in range(1,int(value)+1):
        urls.append(url + '/' + str(i))
    
    return urls


def fetch_data(urls):
    reqs = [grequests.get(url) for url in urls]
    resp = grequests.map(reqs)
    return resp


def parser(resp):
    items = []
    for r in resp:
        soup = bs(r.content , 'lxml')
        all_games = soup.find_all('li',class_= "psw-l-w-1/2@mobile-s psw-l-w-1/2@mobile-l psw-l-w-1/6@tablet-l psw-l-w-1/4@tablet-s psw-l-w-1/6@laptop psw-l-w-1/8@desktop psw-l-w-1/8@max")
        for game in all_games:
            item = {'name':game.find(class_ = 'psw-t-body psw-c-t-1 psw-t-truncate-2 psw-m-b-2').text}
            try:
                item['newprice'] = game.find(class_ = 'psw-m-r-3').text.replace('$', '') + '$'
                item['oldprice'] = game.find('s' , class_ = 'psw-c-t-2').text.replace('$', '') + '$'
                item['discount'] = game.find(class_= 'psw-body-2 psw-badge__text psw-badge--none psw-text-bold psw-p-y-0 psw-p-2 psw-r-1 psw-l-anchor').text

                try:
                    item['description'] = game.find(class_ ='psw-truncate-text-1 psw-c-t-2').text
                except:
                    item['description'] = ' '
                
            except:
                pass
            
            items.append(item)
            
    data = pd.DataFrame(items)
    data.drop_duplicates(subset = 'name',inplace = True)
    data.fillna(' ',inplace = True)
    
    return data



def db_conn():
    
    conn =sqlite3.Connection(r'example.db')
    print('Connection Successful...')
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS Deals (ID INTEGER PRIMARY KEY AUTOINCREMENT,Name TEXT,Newprice TEXT,Oldprice TEXT,Discount TEXT,Description TEXT)')
    conn.commit()
    
    urls = get_urls()
    resp=fetch_data(urls)
    data = parser(resp)
    data.to_sql('Deals', conn, if_exists='replace')
    conn.commit()
    conn.close()
    print('End of ETL Process...')
    return 


if __name__ == '__main__':
    start_time = time.perf_counter()
    db_conn()
    print(time.perf_counter() - start_time)
