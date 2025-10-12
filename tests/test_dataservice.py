#!/usr/bin/env python
from pathlib import Path
import json
import csv
import re
import shutil
from ctrack.check_phases import match_input_to_accounts, check_account_matcher
from ctrack.cc_file_ops import find_card_files

from ctrack.data_service import DataService

def test_account_matching():
    #pull_dir = Path(__file__).parent / "prep_data" / "test_input_matching"
    pull_dir = Path(__file__).parent / "prep_data" / "test_accounts_in_matcher"
    data_dir = Path(__file__).parent / "target"
    for item in data_dir.glob("*"):
        print(f"removing {item}")
        item.unlink()
    for item in pull_dir.glob("*"):
        shutil.copy(item, data_dir)
    dataservice = DataService(data_dir)

    dataservice.load_matcher_file(data_dir / "matcher_map.csv")
    dataservice.load_gnucash_file(data_dir / "test.gnucash")

    found = []
    missing = []
    for matcher in dataservice.get_matchers():
        accnt = dataservice.get_account(matcher.account_path)
        if accnt is None:
            missing.append(matcher)
        else:
            found.append(accnt)

    print('-'*120)
    print("\n".join([accnt.account_path for accnt in found]))
    print('-'*120)
    print("\n".join([str(m) for m in missing]))

    
def test_full_flow():

    # 1. Import accounts
    # 2. Load Matchers
    # 3. Load a transaction file that maps to known column map
    # 4. Find transactions with no matchers
    # 5. Add matchers (updates internal accounts)
    # 6. Update gnucashe accounts to include new accounts
    # 7. Make "standardized" importable transactions file
    # 8. Load a transaction file with no column map
    # 9. Update column map using transaction file record for non-mapped file
    # 10. Reprocess transaction file
    # 11. Write standardized transactions file
    
    pull_dir = Path(__file__).parent / "prep_data" / "test_full_flow"
    data_dir = Path(__file__).parent / "target"
    for item in data_dir.glob("*"):
        print(f"removing {item}")
        item.unlink()
    for item in pull_dir.glob("*"):
        shutil.copy(item, data_dir)
    dataservice = DataService(data_dir)

    dataservice.load_gnucash_file(data_dir / "test.gnucash")
    dataservice.load_matcher_file(data_dir / "matcher_map.csv")

    dataservice.load_transactions(data_dir / "cc_one_match_one_miss.csv")

    dataservice.load_transactions(data_dir / "cc_no_col_map.csv")

    dataservice.load_transactions(data_dir / "cc_with_payment.csv")
        
