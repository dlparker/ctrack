import csv
from pprint import pprint
from collections import defaultdict
from pathlib import Path
import json
from ctrack.make_map import build_account_matchers, map_card_input_accounts
from ctrack.account_sync import export_gnucash_accounts, update_gnucash_accounts
from ctrack.cc_file_ops import find_card_files

DEBUG_PRINT=False

def check_10(data_dir):
    by_card = find_card_files(data_dir, pattern="cc_*.csv")
    matcher_map_csv_path = data_dir / "matcher_map.csv"
    account_matchers = build_account_matchers(matcher_map_csv_path)
    miss_match = defaultdict(list)
    for card, data in by_card.items():
        mm_count = len(miss_match)
        for spec in data.values():
            cat_map = map_card_input_accounts(spec['path'], spec['col_map'], account_matchers)
            for desc,item in cat_map.items():
                if item is None:
                    miss_match[desc].append(str(spec['path']))
    with open(data_dir / "no_match_results.csv", 'w') as f:
        fieldnames = ['description', 'files']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for desc, files in miss_match.items():
            writer.writerow({'description': desc, 'files': json.dumps(files)})
    return miss_match

def check_20(gnucash_path, data_dir):
    acc_defs_path = Path(data_dir) / "account_defs.csv"
    export_gnucash_accounts(gnucash_path, acc_defs_path)
    matcher_map_csv_path = Path(data_dir) / "matcher_map.csv"
    account_matchers = build_account_matchers(matcher_map_csv_path)
    accounts = {}
    no_match_accounts = []
    no_account_matchers = []
    with open(acc_defs_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            accounts[row['account_path']] = row
            found = False
            for matcher in account_matchers:
                if matcher.value == row['account_path']:
                    found = True
                    break
            if not found:
                no_match_accounts.append(row)

    for matcher in account_matchers:
        if matcher.value not in accounts:
            no_account_matchers.append(matcher)

    res_path = data_dir / "new_account_paths.csv"
    with open(res_path, 'w') as f:
        fieldnames = ['account_path', 'cc_desc_re']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in no_account_matchers:
            writer.writerow({'account_path': row.value, 'cc_desc_re': row.re_str})
    if DEBUG_PRINT:
        print("Accounts missing from matchers (may be ok if not leaf):")
        for acc in no_match_accounts:
            print(acc)
        print("-"*100)
        print("Matchers naming non-existing accounts:")
        for m in no_account_matchers:
            print(m)
    return no_match_accounts, no_account_matchers, res_path

