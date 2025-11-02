#!/usr/bin/env python
from pathlib import Path
import json
import csv
import re
import shutil
from decimal import Decimal
import pytest

from ctrack.flow import MainFlow, DataNeeded, NextStep

    
def test_full_flow():

    pull_dir = Path(__file__).parent / "prep_data" / "test_full_flow"
    data_dir = Path(__file__).parent / "target"
    for item in data_dir.glob("*"):
        #print(f"removing {item}")
        item.unlink()
    for item in pull_dir.glob("*"):
        shutil.copy(item, data_dir)

    flow = MainFlow(data_dir)
    assert DataNeeded.GNUCASH in flow.get_data_needs()

    assert NextStep.SET_GNUCASH == flow.get_next_step()
    flow.set_gnucash(data_dir / "test.gnucash")
    assert DataNeeded.GNUCASH not in flow.get_data_needs()
    assert DataNeeded.XACTION_FILE in flow.get_data_needs()
    assert NextStep.LOAD_XACTION_FILE == flow.get_next_step()

    flow.add_xaction_file(data_dir / "cc_no_col_map.csv")
    assert DataNeeded.XACTION_FILE not in flow.get_data_needs()
    assert DataNeeded.COLUMN_MAP in flow.get_data_needs()
    unmapped, unmatched = flow.get_unfinished_xactions()
    assert len(unmapped) == 1
    assert NextStep.ADD_COLUMN_MAP == flow.get_next_step()
    flow.add_column_map('map2',"Date", "Payee", "Amount", "%m/%d/%Y")
    assert DataNeeded.COLUMN_MAP not in flow.get_data_needs()

    assert NextStep.ADD_MATCHER_RULE == flow.get_next_step()
    assert DataNeeded.MATCHER_RULE in flow.get_data_needs()
    account_name = "Expenses:books:on_line"
    matcher = flow.add_matcher_rule(regexp="^kindle", no_case=True, account_name=account_name)
    assert DataNeeded.MATCHER_RULE not in flow.get_data_needs()

    assert NextStep.ADD_ACCOUNT == flow.get_next_step()
    assert DataNeeded.ACCOUNT in flow.get_data_needs()
    missing,unsaved = flow.get_missing_accounts()
    assert account_name in missing
    flow.add_account(name=account_name, description=f"Test saved account {account_name}")
    assert DataNeeded.ACCOUNT not in flow.get_data_needs()
    missing,unsaved = flow.get_missing_accounts()
    assert account_name not in missing
    assert account_name in unsaved

    # not yet saved to gnu cash
    assert NextStep.DO_ACCOUNT_SYNC == flow.get_next_step()
    assert DataNeeded.ACCOUNT_SYNC in flow.get_data_needs()
    flow.dataservice.save_account(account_name)
    assert DataNeeded.ACCOUNT_SYNC not in flow.get_data_needs()
    ready = flow.get_savable_xactions()
    assert len(ready) == 1

    flow.add_xaction_file(data_dir / "cc_one_match_one_miss.csv")
    assert DataNeeded.MATCHER_RULE in flow.get_data_needs()

    unmapped, unmatched = flow.get_unfinished_xactions()
    assert len(unmapped) == 0
    assert len(unmatched) == 1
    
    flow.load_matcher_rules_file(data_dir / "matcher_map_short.csv")
    # should be added to transactions
    assert DataNeeded.MATCHER_RULE not in flow.get_data_needs()
    
    assert DataNeeded.ACCOUNT in flow.get_data_needs()
    g_account_name = "Expenses:groceries:heb:online_groceries"
    flow.add_account(name=g_account_name, description=f"Test saved account {account_name}", save=True)
    assert DataNeeded.ACCOUNT not in flow.get_data_needs()
    assert DataNeeded.ACCOUNT_SYNC not in flow.get_data_needs()
    
    assert NextStep.SAVE_XACTIONS == flow.get_next_step()
    ready = flow.get_savable_xactions()
    assert len(ready) == 2
    payments_account = 'Assets:Checking:PendingChecks'
    for sfile in ready:
        assert NextStep.SAVE_XACTIONS == flow.get_next_step()
        sfile.save_to_gnucash(flow.dataservice, "Liabilities:MC1", payments_account)
    assert NextStep.LOAD_XACTION_FILE == flow.get_next_step()
