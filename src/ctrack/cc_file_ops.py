#!/usr/bin/env python
import re
import csv
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

card_file_col_maps = {
    'boa': {
            "date_col_name": "Posted Date",
            "desc_col_name": "Payee",
            "amt_col_name": "Amount",
            "date_format": "%m/%d/%Y"
            }
}

def find_card_files(data_dir, pattern="*cc_.csv"):
    by_card = defaultdict(dict)
    for csvpath in data_dir.glob(pattern):
        tmp = csvpath.stem.split("_")
        myear = tmp[0]
        card_tail = tmp[-1] # we are assuming last four digits are last four card number digits.
        with open(csvpath) as f:
            csv_reader = csv.DictReader(f)
            fieldnames = csv_reader.fieldnames
            col_map = None
            month = None
            for cm_name, cm in card_file_col_maps.items():
                if cm['date_col_name'] in fieldnames and cm['desc_col_name'] in fieldnames:
                    row = next(csv_reader)
                    try:
                        row_one_date = datetime.strptime(row[cm['date_col_name']], cm['date_format'])
                        month = row_one_date.month
                        col_map = cm
                        break
                    except ValueError:
                        print(f'trying column map {cm_name} failed, column "{cm['date_col_name']}" does ' /
                              f'not contain a date in format {cm["date_format"]}')
            if col_map is None:
                raise Exception(f"Could not match file {path} with a known file type")
            by_card[card_tail][month] = {'path': csvpath, 'col_map': col_map}
    for card, data in by_card.items():
        by_card[card] = dict(sorted(data.items()))
        
    return by_card

def map_accounts(fpath, col_map, account_matchers):

    path = Path(fpath)

    items = []
    with open(path) as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            items.append(row[col_map['desc_col_name']])
    res_dict = {}
    for item in items:
        accnt_name = None
        for matcher in account_matchers:
            if matcher.re_compiled.match(item):
                accnt_name = matcher.value
                break
        res_dict[item] = accnt_name
    return res_dict
                

    
