"""
Code Stage: Solving
"""
import re
import csv
from pprint import pformat
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from ctrack.make_map import build_account_matchers,map_card_input_line_account

card_file_col_maps = {
    'boa': {
            "date_col_name": "Posted Date",
            "desc_col_name": "Payee",
            "amt_col_name": "Amount",
            "date_format": "%m/%d/%Y"
            }
}

def find_card_files(data_dir, pattern="*cc_*.csv"):
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

    
def convert_card_files(data_dir, out_dir, pattern="*cc_*.csv"):
    data_dir = Path(data_dir)
    out_dir = Path(out_dir)
    if data_dir == out_dir:
        prefix = "mod_"
    else:
        prefix = ""
    by_card = find_card_files(data_dir, pattern)
    matcher_map_csv_path = data_dir / "matcher_map.csv"
    account_matchers = build_account_matchers(matcher_map_csv_path)
    for card, data in by_card.items():
        for spec in data.values():
            new_lines = []
            col_map = spec['col_map']
            print(spec['path'])
            with open(spec['path']) as f:
                csv_reader = csv.DictReader(f)
                for row in csv_reader:
                    if float(row[col_map['amt_col_name']]) > 0:
                        continue
                    desc = row[col_map['desc_col_name']]
                    account_path = map_card_input_line_account(desc, account_matchers)
                    if account_path is None:
                        raise Exception(f'cannot convert files, missing accounts for "{desc}"')
                    new_row = dict(row)
                    new_row['gnucash_account'] = account_path
                    new_lines.append(new_row)
            new_file = out_dir / f"{prefix}{Path(spec['path']).stem}.csv"
            print(new_file)
            with open(new_file, 'w') as f:
                fieldnames = ["Date", "Description", "Amount", "Account"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in new_lines:
                    new_row = {
                        'Date': row[col_map['date_col_name']],
                        'Description': row[col_map['desc_col_name']],
                        'Amount': row[col_map['amt_col_name']],
                        'Account': row['gnucash_account']
                        }
                    writer.writerow(new_row)
                        
                
