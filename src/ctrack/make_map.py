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

@dataclass
class AccountMatcher:
    re_str: str
    value: str
    re_compiled: re.Pattern

def build_account_matchers(path):
    res = []
    with open(path) as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            re_str = row['cc_desc_re']
            if row['re_no_case'] == "True": 
                re_compiled = re.compile(re_str, re.IGNORECASE)
            else:
                re_compiled = re.compile(re_str)
            value =  row['account_path']
            res.append(AccountMatcher(re_str, value, re_compiled))
    return res

def map_card_input_accounts(fpath, col_map, account_matchers):

    path = Path(fpath)

    items = []
    with open(path) as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            if float(row[col_map['amt_col_name']]) > 0:
                continue
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
                

if __name__=="__main__":
    from pprint import pprint
    data_dir = Path(__file__).parent / "test_data"
    by_card = find_card_files(data_dir)
    pprint(by_card)

    for card, data in by_card.items():
        print('-'*120)
        print(f"Card = {card}")
        cat_map = dict()
        for spec in data.values():
            cat_map = map_accounts(spec['path'], spec['col_map'], spec['account_matchers'])
        print(f"End Card = {card}")
        pprint(cat_map)
    
    

    
