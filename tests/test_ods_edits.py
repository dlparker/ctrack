#!/usr/bin/env python
import shutil
from pathlib import Path
from ctrack.check_phases import check_account_matcher, match_input_to_accounts
from ctrack.cc_file_ops import convert_card_files
# for matcher edit
from ctrack.ods_ops import setup_ods_matcher_edit, apply_ods_matcher_edit, run_ods_matcher_edit
# for accounts edit
from ctrack.ods_ops import setup_ods_accounts_edit, apply_ods_accounts_edit


def test_edit_ods_matcher():
    pull_dir = Path(__file__).parent / "prep_data" / "test_edit_ods_matcher"
    data_dir = Path(__file__).parent / "target"
    for item in data_dir.glob("*"):
        item.unlink()
    for item in pull_dir.glob("*"):
        shutil.copy(item, data_dir)
    orig_map_path = data_dir / "matcher_map_orig.csv"
    map_path = data_dir / "matcher_map.csv"
    shutil.copy(orig_map_path, map_path)
    misses = match_input_to_accounts(data_dir)
    setup_ods_matcher_edit(data_dir)
    # in command line tool do this:
    # edit_10_run_calc(data_dir)

    # simulate human fixing things in ods file
    edited_path = Path(data_dir) / "edit_10_edited.ods"
    op_path = Path(data_dir) / "edit_10.ods"
    shutil.copy(edited_path, op_path)

    # now make sure that it did fix things
    apply_ods_matcher_edit(data_dir)
    finished_misses = match_input_to_accounts(data_dir)
    assert len(finished_misses) == 0
    
        
def test_edit_ods_accounts():

    pull_dir = Path(__file__).parent / "prep_data" / "test_edit_ods_accounts"
    data_dir = Path(__file__).parent / "target"
    for item in data_dir.glob("*"):
        item.unlink()
    for item in pull_dir.glob("*"):
        shutil.copy(item, data_dir)
    orig_path = data_dir / "orig_test.gnucash"
    gnucash_path = data_dir / "test.gnucash"
    shutil.copy(orig_path, gnucash_path)
    no_match_accounts, no_account_matchers, res_path = check_account_matcher(gnucash_path, data_dir)
    new_accounts_file = data_dir / "new_account_paths.csv"
    edit_ods = setup_ods_accounts_edit(data_dir, gnucash_path, new_accounts_file)
    #print(edit_ods)
    
    # in command line tool do this:
    # edit_20_run_calc(data_dir)

    # simulate human fixing things in ods file
    edited_path = Path(data_dir) / "edited_new_accounts.ods"
    op_path = Path(data_dir) / "new_accounts.ods"
    shutil.copy(edited_path, op_path)

    # now make sure that it did fix things
    apply_ods_accounts_edit(data_dir, gnucash_path)
    convert_card_files(data_dir, "/tmp")


    
        
    
