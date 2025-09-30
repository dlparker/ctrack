#!/usr/bin/env python
import shutil
import csv
from pathlib import Path
from ctrack.check_phases import check_account_matcher
from ctrack.edit_phases import setup_ods_accounts_edit

def test_check_20_ods():
    pull_dir = Path(__file__).parent / "data_test_check_20"
    data_dir = Path(__file__).parent / "target"
    for item in data_dir.glob("*"):
        item.unlink()
    for item in pull_dir.glob("*"):
        shutil.copy(item, data_dir)
    test_path = data_dir / "test.gnucash"
    no_match_accounts, no_account_matchers, res_path = check_account_matcher(test_path, data_dir)
    saved_new = []
    with open(res_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            saved_new.append(row['account_path'])
   
    expected_new = []
    expected_new_path = data_dir / "expected_new_paths.csv"
    with open(expected_new_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            expected_new.append(row['account_path'])
    
    for item in expected_new:
        assert item in saved_new

    for item in saved_new:
        assert item in expected_new

    
        
