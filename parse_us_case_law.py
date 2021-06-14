"""
Downloading the case.law texts:

wget https://case.law/download/manifest.csv

ag bulk_exports/latest/by_jurisdiction manifest.csv --nonumbers > bulk_urls.txt

cut -d',' -f1 <bulk_urls.txt > urls.txt

while IFS="" read -r p || [ -n "$p" ]; do
    wget --mirror --header="Authorization: Token 309995e447ea7a7b0f90a6a9f62732638c2f6d80" "case.law/download/${p}";
done < urls.txt
"""
import argparse
import requests, shutil, zipfile
from bs4 import BeautifulSoup
import glob
import os
from tqdm import tqdm
import json_lines
from data import DATA_DIR

dir_root = os.path.join(DATA_DIR, 'us_caselaw')


def parse_us_caselaw(input_path):
    case_text_paths = ["download/bulk_exports/latest/by_jurisdiction/case_text_open/*",
                       "download/bulk_exports/latest/by_jurisdiction/case_text_restricted/*"]
    for case_text_path in case_text_paths:
        for state in tqdm(os.path.join(input_path, case_text_path)):
            if not os.path.isdir(state):
                continue
            if state.startswith("."):
                continue
            print(f"Processing state {state}")
            state_zip = os.path.join(state, f"{state}_text.zip")
            with zipfile.ZipFile(state_zip) as zf:
                state_tmp = os.path.join(dir_root, f'{state}_temp')
                zf.extractall(state_tmp)
            filenames = glob.glob(f'{state_tmp}/*/data/*')
            jsonl_filename = os.path.join(DATA_DIR, f'{state}.jsonl.xz')
            shutil.move(filenames[0], jsonl_filename)
            shutil.rmtree(state_tmp)
            os.makedirs(os.path.join(dir_root, state))
            with json_lines.open(jsonl_filename) as reader:
                for obj in reader:
                    id = obj['id']
                    title = obj['reporter']['full_name'] + ' ' + obj['citations'][0]['cite']
                    head = obj['casebody']['data']['head_matter']
                    opinions = [op['text'] for op in obj['casebody']['data']['opinions']]
                    if len(opinions) > 1:
                        opinions = '\n\n'.join(opinions)
                    elif len(opinions) == 1:
                        opinions = opinions[0]
                    else:
                        opinions = ''
                    if len(obj['casebody']['data']['judges']) != 0:
                        judges = obj['court']['name'] + '\n' + obj['casebody']['data']['judges'][0]
                    else:
                        judges = obj['court']['name']
                    keys = list(obj['casebody']['data'].keys())
                    if len(keys) > 5:
                        print('ERROR {}'.format(id))

                    with open(os.path.join(dir_root, state, f'{id}.txt'), 'w') as file:
                        file.write(title + '\n' + head + '\n' + opinions + '\n' + judges)
            os.remove(jsonl_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=("Given a text file, calculate the MLM loss."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-path", type=str, required=True, help=("Path to case.law mirror with text of cases.")
    )
    args = parser.parse_args()
    parse_us_caselaw(args.input_path)
