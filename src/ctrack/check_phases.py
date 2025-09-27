import csv
from pprint import pprint
from collections import defaultdict
import json
from ctrack.make_map import build_account_matchers, map_card_input_accounts
from ctrack.cc_file_ops import find_card_files
from pathlib import Path

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
