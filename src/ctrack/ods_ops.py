"""
Code Stage: Solving
"""
import subprocess
import json
import csv
from pathlib import Path
from collections import OrderedDict
from pyexcel_ods3 import save_data
import pyexcel as pe
from ctrack.make_map import update_account_matchers
from ctrack.account_sync import update_gnucash_accounts

def setup_ods_matcher_edit(data_dir):
    data = OrderedDict() # from collections import OrderedDict

    max_file_count = 0
    no_match_data = []
    with open(Path(data_dir) / "no_match_results.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ods_row = [f'^{row["description"]}', 'True', 'Expenses', row['description'],]
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

def run_ods_matcher_edit(data_dir): # pragma: no cover
    path = Path(data_dir) / "edit_10.ods"
    if not path.exists():
        raise Exception(f'no file {path}')
    p = subprocess.run(["libreoffice", "--calc", str(path)])
    
def apply_ods_matcher_edit(data_dir):
    path = Path(data_dir) / "edit_10.ods"
    if not path.exists():
        raise Exception(f'no file {path}')
    book = pe.get_book(file_name=path)
    sheet = book.bookdict['No Match']

    new_matchers = []
    for index,row in enumerate(sheet):
        if index == 0:
            continue
        regexp, ignorecase, account_path = row[:3]
        if regexp.strip() != '':
            new_matchers.append([regexp, ignorecase, account_path])
    update_account_matchers(Path(data_dir) / "matcher_map.csv", new_matchers)

def setup_ods_accounts_edit(data_dir, gnucash_path):
    data = OrderedDict() # from collections import OrderedDict

    headers = ['account_path','description']
    new_accounts = [headers,]
    a_set = set()
    new_accounts_path = data_dir / "new_account_paths.csv"
    with open(new_accounts_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            a_set.add(row['account_path'])

    for path in list(a_set):
        new_accounts.append([path, ''])
    data.update({"New Accounts": new_accounts})
    dfile = data_dir / "new_accounts.ods"
    save_data(str(dfile), data)
    return dfile

def run_ods_accounts_edit(data_dir): # pragma: no cover
    path = Path(data_dir) / "new_accounts.ods"
    if not path.exists():
        raise Exception(f'no file {path}')
    p = subprocess.run(["libreoffice", "--calc", str(path)])

def apply_ods_accounts_edit(data_dir, gnucash_path):
    path = Path(data_dir) / "new_accounts.ods"
    if not path.exists():
        raise Exception(f'no file {path}')
    book = pe.get_book(file_name=path)
    sheet = book.bookdict['New Accounts']

    new_accounts = []
    for index,row in enumerate(sheet):
        if index == 0:
            continue
        account_path, description = row[:2]
        new_accounts.append({"account_path": account_path, "description": description})
        
    update_gnucash_accounts(gnucash_path, new_accounts)

