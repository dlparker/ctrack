import subprocess
import json
import csv
from pathlib import Path
from collections import OrderedDict
from pyexcel_ods3 import save_data
import pyexcel as pe
from ctrack.make_map import update_account_matchers

def edit_10_gen_ods(data_dir):
    data = OrderedDict() # from collections import OrderedDict

    max_file_count = 0
    no_match_data = []
    with open(Path(data_dir) / "no_match_results.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ods_row = ['', 'True', 'Expense', row['description'],]
            no_match_data.append(ods_row)
            file_count = 0
            for path in json.loads(row['files']):
                ods_row.append(path)
                file_count += 1
            max_file_count = max(max_file_count, file_count)
                
    headers = ['regexp','ignorecase', 'account_path', 'description']
    for i in range(max_file_count):
        headers.append(f'file_{i}')
    no_match_data.insert(0, headers)
    data.update({"No Match": no_match_data})
    save_data(str(data_dir / "edit_10.ods"), data)

def edit_10_reload_ods(data_dir):
    path = Path(data_dir) / "edit_10.ods"
    if not path.exists():
        raise Exception(f'no file {path}')
    book = pe.get_book(file_name=path)
    sheet = book.bookdict['No Match']

    new_matchers = []
    for index,row in enumerate(sheet):
        regexp, ignorecase, account_path = row[:3]
        if regexp.strip() != '':
            new_matchers.append([regexp, ignorecase, account_path])
    update_account_matchers(Path(data_dir) / "matcher_map.csv", new_matchers)

def edit_10_run_calc(data_dir):
    path = Path(data_dir) / "edit_10.ods"
    if not path.exists():
        raise Exception(f'no file {path}')
    p = subprocess.run(["libreoffice", "--calc", str(path)])
    
    
