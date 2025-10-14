#!/usr/bin/env python
from pathlib import Path
import json
import csv
import re
import shutil
from decimal import Decimal
import pytest

from ctrack.data_service import DataService

    
def test_full_flow():

    # 1. Import accounts
    # 2. Load Matchers
    # 3. Load a transaction file that maps to known column map
    # 4. Find transactions with no matchers
    # 5. Add matcher
    # 6. Add account for new matcher
    # 7. Update gnucash accounts to include new account
    # 8. Make "standardized" importable transactions file
    # 9. Load a transaction file with no column map
    # 10. Add new column map 
    # 11. Reprocess transaction file
    # 12. Make sure payment does not break anything
    # 13. Make "standardized" importable transactions file and ensure it handled payment
    
    pull_dir = Path(__file__).parent / "prep_data" / "test_full_flow"
    data_dir = Path(__file__).parent / "target"
    for item in data_dir.glob("*"):
        #print(f"removing {item}")
        item.unlink()
    for item in pull_dir.glob("*"):
        shutil.copy(item, data_dir)
    dataservice = DataService(data_dir)

    # 1. Import accounts
    dataservice.load_gnucash_file(data_dir / "test.gnucash")
    assert dataservice.accounts_count() == 1
    expected_accounts = ['Expenses:House:Insurance']
    found_accounts = []
    for account in dataservice.get_accounts():
        found_accounts.append(account.name)
        assert account.name in expected_accounts
    
    # 2. Load Matchers
    dataservice.load_matcher_file(data_dir / "matcher_map.csv")
    assert dataservice.matchers_count() == 8

    # 3. Load a transaction file that maps to known column map
    one_miss_first = dataservice.load_transactions(data_dir / "cc_one_match_one_miss.csv")

    # 4. Find transactions with no matchers
    for xact in dataservice.get_transactions(one_miss_first):
        if "heb" in xact.description.lower():
            assert xact.matcher_id is not None
        elif "kindle" in xact.description.lower():
            assert xact.matcher_id is None
            kindle_row = xact

    # Load the same transaction file again and perform same checks
    one_miss_file = dataservice.load_transactions(data_dir / "cc_one_match_one_miss.csv")
    for xact in dataservice.get_transactions(one_miss_file):
        if "heb" in xact.description.lower():
            assert xact.matcher_id is not None
        elif "kindle" in xact.description.lower():
            assert xact.matcher_id is None
            kindle_row = xact
    
    # Make sure it bitches when we try to convert an unfinished file
    with pytest.raises(Exception):
        dataservice.standardize_transactions(one_miss_file)

    # 5. Add matcher
    account_name = "Expenses:books:on_line"
    matcher = dataservice.add_matcher(regexp="^kindle", no_case=True, name=account_name)
    assert account_name in str(matcher)
    assert matcher.compiled.match(xact.description)
    xact.matcher_id = matcher.id
    new_xact = dataservice.update_transaction_matcher(xact)
    assert new_xact.matcher_id == matcher.id

    # 6. Add account for new matcher
    missing = set()
    for matcher in dataservice.get_matchers():
        accnt =  dataservice.get_account(matcher.account_name)
        assert accnt is None
        missing.add(matcher.account_name)
    for path in list(missing):
        dataservice.add_account(path, f"Test inserted {path}")
    for matcher in dataservice.get_matchers():
        accnt =  dataservice.get_account(matcher.account_name)
        assert accnt is not None
    
    # 7. Update gnucash accounts to include new account
    dataservice.update_gnucash_accounts()
    # reload accounts from file so we can verify
    dataservice.load_gnucash_file(data_dir / "test.gnucash")
    for matcher in dataservice.get_matchers():
        accnt =  dataservice.get_account(matcher.account_name)
        assert accnt is not None
        assert accnt.in_gnucash

    # 8. Make "standardized" importable transactions file
    output_data = dataservice.standardize_transactions(one_miss_file)
    for row in output_data:
        assert  row['GnucashAccount'] is not None

    # 9. Load a transaction file with no column map
    no_map_file = dataservice.load_transactions(data_dir / "cc_no_col_map.csv")
    assert len(dataservice.get_transactions(no_map_file)) == 0
    
    # 10. Add new column map
    assert len(dataservice.get_column_maps()) == 1
    dataservice.add_column_map('map2',"Date", "Payee", "Amount", "%m/%d/%Y")
    assert len(dataservice.get_column_maps()) == 2


    # 11. Reprocess transaction file
    no_map_file = dataservice.reload_transactions(data_dir / "cc_no_col_map.csv")
    assert len(dataservice.get_transactions(no_map_file)) == 1

    # 12. Make sure payment does not break anything
    pay_file = dataservice.load_transactions(data_dir / "cc_with_payment.csv")
        
    # 13. Make "standardized" importable transactions file and ensure it handled payment
    payments_account = 'Assets:Checking:PendingChecks'
    # make sure bogus args explode
    with pytest.raises(Exception):
        dataservice.standardize_transactions(pay_file, data_dir / "import_cc_with_payment.csv",
                                             include_payments=True, payments_account=None) 
    output_data = dataservice.standardize_transactions(pay_file, data_dir / "import_cc_with_payment.csv",
                                                       include_payments=True, payments_account=payments_account)
    # should have two charge rows, one payment
    assert len(output_data) == 3
    for index,row in enumerate(output_data):
        if index < 2:
            assert  len(row['GnucashAccount']) > 0
        else:
            assert row['GnucashAccount'] == payments_account

    # try it with ignore payments
    output_data_2 = dataservice.standardize_transactions(pay_file, include_payments=False)
    assert len(output_data_2) == 2
    
    # make sure bogus args explode
    with pytest.raises(Exception):
        dataservice.do_cc_transactions(pay_file, cc_name="Liabilities:MC1", 
                                              include_payments=True)
    balances = dataservice.do_cc_transactions(pay_file, cc_name="Liabilities:MC1", 
                                              include_payments=True, payments_name=payments_account)
    from pprint import pprint
    pprint(balances)
    assert balances["Liabilities:MC1"] == Decimal('0.00')
    assert balances[payments_account] == Decimal('1.00')
    assert balances['Expenses:books:on_line'] == Decimal('12.98')
    assert balances['Expenses:groceries:heb:online_groceries'] == Decimal('151.84')
            
