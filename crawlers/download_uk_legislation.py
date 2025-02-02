import os
import sys
import requests
from multiprocessing import cpu_count, Pool
from data import DATA_DIR
from bs4 import BeautifulSoup
from crawlers.helpers import clean_text
import traceback
from tqdm import tqdm
from fake_useragent import UserAgent
from time import sleep
sys.setrecursionlimit(100000)

dir_root = os.path.join(DATA_DIR, 'uk')
ua = UserAgent()

if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)


def get_file_by_id(original_url):
    header = {'User-Agent':str(ua.chrome)}
    url = original_url + '/enacted?view=plain'
    uk_id = original_url.replace('https://legislation.gov.uk/','')
    filename = os.path.join(dir_root, f'{uk_id}.txt')
    try:
        content = requests.get(url, headers=header).text
        sleep(1)
        if 'This item of legislation isn’t available on this site' in content or 'View PDF' in content:
            print(url + ' is not available on the site or is only in PDF')
            return
        elif 'The page you requested could not be found' in content:
            print(url + ' is not available')
            return
        content = clean_text(content)
        content = BeautifulSoup(content, "lxml").find("div", {"id": "content"})
        if content:
            cleantext = content.text
            if cleantext:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(cleantext)
    except Exception:
        print('Unhandled exception: ' + original_url)
        traceback.print_exc()


def download_uk_law():
    types_dict = {'ukpga': (1980, 60), 'ukla': (1991, 60), 'uksi': (1987, 3000), 'asp': (1999, 60), 'ssi': (1999, 600),
                  'wsi': (1999, 350), 'nisi': (1999, 20), 'nia': (2000, 60), 'mwa': (2008, 60), 'ukmo': (2013, 60),
                  'anaw': (2012, 60)}

    possible_links = []
    for act_type, (start_year, last_id) in types_dict.items():
        for year in range(start_year, 2022):
            if not os.path.exists(os.path.join(dir_root, act_type, str(year))):
                os.makedirs(os.path.join(dir_root, act_type, str(year)))
            for id in range(1, last_id + 1):
                possible_links.append(f'https://legislation.gov.uk/{act_type}/{year}/{id}')

    with Pool(processes=cpu_count()) as pool:
        with tqdm(total=len(possible_links)) as pbar:
            for _ in pool.imap_unordered(get_file_by_id, possible_links):
                pbar.update()

if __name__ == '__main__':
    download_uk_law()
