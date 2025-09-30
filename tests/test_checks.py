#!/usr/bin/env python
from pathlib import Path
import json
import csv
import re
import shutil
from ctrack.check_phases import match_input_to_accounts, check_account_matcher
from ctrack.cc_file_ops import find_card_files

def test_input_matching():
    pull_dir = Path(__file__).parent / "prep_data" / "test_input_matching"
    data_dir = Path(__file__).parent / "target"
    for item in data_dir.glob("*"):
        item.unlink()
    for item in pull_dir.glob("*"):
        shutil.copy(item, data_dir)
    misses = match_input_to_accounts(data_dir)

    # first make sure that file contains all the missed items
    from_file = {}
    with open(data_dir / "no_match_results.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            from_file[row['description']] = json.loads(row['files'])

    for desc, files in misses.items():
        assert from_file[desc] == files

    # Now make sure that all the missed items fail match with known matchers
    matchers = []
    with open(data_dir / "matcher_map.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            re_str = row['cc_desc_re']
            if row['re_no_case'] == "True": 
                re_compiled = re.compile(re_str, re.IGNORECASE)
            else:
                re_compiled = re.compile(re_str)
            matchers.append(re_compiled)

    for matcher in matchers:
        for desc in misses:
            assert not matcher.match(desc)

def test_accounts_in_matcher():
    pull_dir = Path(__file__).parent / "prep_data" / "test_accounts_in_matcher"
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

    
        
    
