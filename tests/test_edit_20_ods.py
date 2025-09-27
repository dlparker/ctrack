#!/usr/bin/env python
import shutil
from pathlib import Path
from ctrack.check_phases import check_20
from ctrack.edit_phases import edit_20_gen_ods


def test_edit_20_ods():
    data_dir = Path(__file__).parent / "data_test_edit_20"
    orig_path = data_dir / "orig_test.gnucash"
    test_path = data_dir / "test.gnucash"
    shutil.copy(orig_path, test_path)
    no_match_accounts, no_account_matchers, res_path = check_20(test_path, data_dir)
    new_accounts_file = data_dir / "new_account_paths.csv"
    edit_ods = edit_20_gen_ods(data_dir, test_path, new_accounts_file)
    print(edit_ods)
    

    
        
