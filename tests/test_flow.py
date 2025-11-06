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
        item.unlink()
    for item in pull_dir.glob("*"):
        shutil.copy(item, data_dir)

    # If the database is new, no gnu cash file has been identified, we
    # should see that as the required first step
    flow = MainFlow(data_dir)
    assert DataNeeded.GNUCASH in flow.get_data_needs()

    assert NextStep.SET_GNUCASH == flow.get_next_step()
    flow.set_gnucash(data_dir / "test.gnucash")
    assert DataNeeded.GNUCASH not in flow.get_data_needs()

    # Once a gnucash file is identified, either as above or
    # because one is already in the DB, if there is no
    # unfinished work to do, then we should see that loading
    # a transaction file is the next step
    assert DataNeeded.XACTION_FILE in flow.get_data_needs()
    assert NextStep.LOAD_XACTION_FILE == flow.get_next_step()
    flow.add_xaction_file(data_dir / "cc_no_col_map.csv")
    assert DataNeeded.XACTION_FILE not in flow.get_data_needs()

    # If the loaded file does not map known column mapping,
    # then we should see that adding a colum map is the next step
    assert DataNeeded.COLUMN_MAP in flow.get_data_needs()
    unmapped, unmatched = flow.get_unfinished_xactions()
    assert len(unmapped) == 1
    assert NextStep.ADD_COLUMN_MAP == flow.get_next_step()
    flow.add_column_map('map2',"Date", "Payee", "Amount", "%m/%d/%Y")
    assert DataNeeded.COLUMN_MAP not in flow.get_data_needs()

    # If the loaded file does match a knonw column map, but
    # it has one or more rows in it that are not matched
    # by any matcher, then we should see taht adding a matcher
    # is the next step. 
    assert NextStep.ADD_MATCHER_RULE == flow.get_next_step()
    assert DataNeeded.MATCHER_RULE in flow.get_data_needs()

    # It is possible to create a matcher "manually" which will be
    # the typical method when a UI is driving the flow code. User
    # looks at the data and creates a new matcher for it. Simulate that.
    account_name = "Expenses:books:on_line"
    matcher = flow.add_matcher_rule(regexp="^kindle", no_case=True, account_name=account_name)
    assert DataNeeded.MATCHER_RULE not in flow.get_data_needs()

    # If the loaded file has all rows matched by some known matcher,
    # but the any matcher refers to an account that does not exist in
    # the gnucash file, then we should be informed that we need
    # to add one or more accounts.
    assert NextStep.ADD_ACCOUNT == flow.get_next_step()
    assert DataNeeded.ACCOUNT in flow.get_data_needs()
    missing,unsaved = flow.get_missing_accounts()
    assert account_name in missing
    flow.add_account(name=account_name, description=f"Test saved account {account_name}")
    assert DataNeeded.ACCOUNT not in flow.get_data_needs()
    missing,unsaved = flow.get_missing_accounts()
    assert account_name not in missing
    assert account_name in unsaved

    # If the file is mapped, fully matched, and all
    # matchers refer to account that have been defined
    # in our DB, but one or more accounts are not present
    # in the gnucash file, we should be informed that we
    # need to sync our local db accounts to the gnucash file.
    assert NextStep.DO_ACCOUNT_SYNC == flow.get_next_step()
    assert DataNeeded.ACCOUNT_SYNC in flow.get_data_needs()
    flow.dataservice.save_account(account_name)
    assert DataNeeded.ACCOUNT_SYNC not in flow.get_data_needs()

    # After all the issues with have been handled, the file should
    # report that it is ready to save in the gnucash file, and the
    # next suggested action (not required, but possible) is to
    # save it.
    ready = flow.get_savable_xactions()
    assert len(ready) == 1
    assert NextStep.SAVE_XACTIONS == flow.get_next_step()
    payments_account = 'Assets:Checking:PendingChecks'
    ready[0].save_to_gnucash("Liabilities:MC1", payments_account)
    ready = flow.get_savable_xactions()
    assert len(ready) == 0

    # All work is complete, so we should be back to needing
    # a transaction file load
    assert DataNeeded.XACTION_FILE in flow.get_data_needs()
    flow.add_xaction_file(data_dir / "cc_one_match_one_miss.csv")
    # If we load a transaction file and there is already a mapping
    # for its columns, but at least one of the rows is missing a
    # matcher, then we should see that a matcher rule add is required.
    assert DataNeeded.MATCHER_RULE in flow.get_data_needs()
    unmapped, unmatched = flow.get_unfinished_xactions()
    assert len(unmapped) == 0
    assert len(unmatched) == 1

    # It is possible to load matchers from csv files, so do that
    # and ensure that the file now reports fully matched.
    flow.load_matcher_rules_file(data_dir / "matcher_map_short.csv")
    # should be added to transactions
    assert DataNeeded.MATCHER_RULE not in flow.get_data_needs()
    unmapped, unmatched = flow.get_unfinished_xactions()
    assert len(unmapped) == 0
    assert len(unmatched) == 0

    # If the loaded file has all rows matched by some known matcher,
    # but the any matcher refers to an account that does not exist in
    # the gnucash file, then we should be informed that we need
    # to add one or more accounts. Do that, but this time let's immediately
    # save it to the gnucash file.
    assert DataNeeded.ACCOUNT in flow.get_data_needs()
    g_account_name = "Expenses:groceries:heb:online_groceries"
    flow.add_account(name=g_account_name, description=f"Test saved account {account_name}", save=True)
    assert DataNeeded.ACCOUNT not in flow.get_data_needs()
    assert DataNeeded.ACCOUNT_SYNC not in flow.get_data_needs()

    # After all the issues with have been handled, the file should
    # report that it is ready to save in the gnucash file, and the
    # next suggested action (not required, but possible) is to
    # save it.
    assert NextStep.SAVE_XACTIONS == flow.get_next_step()
    ready = flow.get_savable_xactions()
    assert len(ready) == 1
    ready[0].save_to_gnucash("Liabilities:MC1", payments_account)
    ready = flow.get_savable_xactions()
    assert len(ready) == 0
    assert NextStep.LOAD_XACTION_FILE == flow.get_next_step()
