"""
Code Stage: Solving
"""
from pathlib import Path
import csv
from piecash import open_book, create_book, Account
import warnings
from sqlalchemy import exc as sa_exc

    
def find_account(parent, name):
    for acc in parent.children:
        if acc.name == name:
            return acc
    return None
        
def update_gnucash_accounts(gnucash_path, account_defs):
    path =  Path(gnucash_path)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa_exc.SAWarning)
        with open_book(str(path), readonly=False) as book:
            USD = book.commodities.get(mnemonic="USD")
            for ac_def in account_defs:
                ac_path = ac_def['account_path']
                ac_desc = ac_def['description']
                parent = book.root_account
                for part in ac_path.split(':'):
                    account = find_account(parent, part)
                    if not account:
                        account = Account(name=part,
                                         type="EXPENSE",
                                         parent=parent,
                                         commodity=USD,
                                         description=ac_desc)
                        print(f"Added account {account}")
                    parent = account
            book.save()
            

def get_account_defs(parent, acc_type, parent_string=None):
    recs = []
    for acc in parent.children:
        if acc.type == acc_type:
            if parent_string is not None:
                string = parent_string + f":{acc.name}"
            else:
                string =  acc.name
            rec = dict(account_path=string,
                       description=acc.description)
            recs.append(rec)
            recs += get_account_defs(acc, acc_type, string)
    return recs
    
def export_gnucash_accounts(gnucash_path, export_path, account_type="EXPENSE"):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa_exc.SAWarning)
        with open_book(str(gnucash_path)) as book:
            parent = book.root_account
            recs = get_account_defs(parent, account_type)
        with open(export_path, 'w', encoding='UTF8', newline='') as f:
            fieldnames = ['account_path', 'description']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(recs)               


