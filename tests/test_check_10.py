#!/usr/bin/env python
from pathlib import Path
import json
import csv
import re
from ctrack.check_phases import check_10
from ctrack.cc_file_ops import find_card_files

def test_check_10():
    data_dir = Path(__file__).parent / "test_check_10"
    misses = check_10(data_dir)

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

    
        
