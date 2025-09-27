#!/usr/bin/env python
from make_map import build_account_matchers, map_card_input_accounts
from cc_file_ops import find_card_files

if __name__=="__main__":
    from pathlib import Path
    import csv
    from pprint import pprint
    from collections import defaultdict
    data_dir = Path(__file__).parent / "test_data/cc_exports"
    by_card = find_card_files(data_dir)
    matcher_map_csv_path = "test_data/matcher_map.csv"
    account_matchers = build_account_matchers(matcher_map_csv_path)
    miss_match = defaultdict(list)
    for card, data in by_card.items():
        mm_count = len(miss_match)
        for spec in data.values():
            cat_map = map_card_input_accounts(spec['path'], spec['col_map'], account_matchers)
            for desc,item in cat_map.items():
                if item is None:
                    miss_match[desc].append(str(spec['path']))
        print(f"Card {card} {len(miss_match) - mm_count} miss matches found ")
    with open("test_data/no_match_results.csv", 'w') as f:
        fieldnames = ['description', 'files']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for desc, files in miss_match.items():
            writer.writerow({'description': desc, 'files':files})

