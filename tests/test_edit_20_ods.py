#!/usr/bin/env python
import shutil
from pathlib import Path
from ctrack.check_phases import check_20
from ctrack.cc_file_ops import convert_card_files
from ctrack.edit_phases import edit_20_gen_ods, edit_20_reload_ods

def test_edit_20_ods():
    data_dir = Path(__file__).parent / "data_test_edit_20"
    orig_path = data_dir / "orig_test.gnucash"
    gnucash_path = data_dir / "test.gnucash"
    shutil.copy(orig_path, gnucash_path)
    no_match_accounts, no_account_matchers, res_path = check_20(gnucash_path, data_dir)
    new_accounts_file = data_dir / "new_account_paths.csv"
    edit_ods = edit_20_gen_ods(data_dir, gnucash_path, new_accounts_file)
    #print(edit_ods)
    
    # in command line tool do this:
    # edit_20_run_calc(data_dir)

    # simulate human fixing things in ods file
    edited_path = Path(data_dir) / "edited_new_accounts.ods"
    op_path = Path(data_dir) / "new_accounts.ods"
    shutil.copy(edited_path, op_path)

    # now make sure that it did fix things
    edit_20_reload_ods(data_dir, gnucash_path)
    convert_card_files(data_dir, "/tmp")


    
        
