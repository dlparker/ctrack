#!/usr/bin/env python
from ctrack.check_phases import check_account_matcher, match_input_to_accounts
from ctrack.ods_ops import setup_ods_matcher_edit, apply_ods_matcher_edit, run_ods_matcher_edit
from ctrack.ods_ops import setup_ods_accounts_edit, apply_ods_accounts_edit, run_ods_accounts_edit


def sync_accounts(cash_file, work_dir):

    no_account_matchers,_,_ = check_account_matcher(cashfile, work_dir)
    while len(no_account_matchers) > 0:
        res = input("Some matchers have no acccount, edit? ")
        if res.lower() not in ['yes', 'y']:
            raise SystemExit(1)
        setup_ods_accounts_edit(work_dir, cash_file)
        run_ods_accounts_edit(work_dir)
        apply_ods_accounts_edit(work_dir, cash_file)
        no_account_matchers,_,_ = check_account_matcher(cashfile, work_dir)

def ensure_matchers(cash_file, work_dir):
    misses = match_input_to_accounts(work_dir)
    while len(misses) > 0:
        res = input("Input cc transaction details have no matcher, edit? ")
        if res.lower() not in ['yes', 'y']:
            raise SystemExit(1)
        setup_ods_matcher_edit(work_dir)
        run_ods_matcher_edit(work_dir)
        apply_ods_matcher_edit(work_dir)
        misses = match_input_to_accounts(work_dir)
        sync_accounts(cash_file, work_dir)

if __name__=="__main__":
    from pathlib import Path
    import shutil
    #import ipdb;ipdb.set_trace()
    parent = Path(__file__).parent.parent
    data_dir = parent / "demo_data"
    work_dir = parent / "demo_work"
    for item in data_dir.glob("*"):
        shutil.copy(item, work_dir)

    cashfile = work_dir / "test.gnucash"
    sync_accounts(cashfile, work_dir)
    ensure_matchers(cashfile, work_dir)
    
