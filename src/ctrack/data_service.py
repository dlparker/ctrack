from pathlib import Path
import csv
import re
import json
import datetime 
from decimal import Decimal
from typing import Optional
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime
import warnings

from piecash import open_book, create_book, Account as CASH_Account
from piecash import Transaction, Split, Commodity
from sqlalchemy.types import TypeDecorator
from sqlalchemy import create_engine, Column, ForeignKey
from sqlalchemy import Boolean, Integer, Date, String, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import exc as sa_exc
from sqlalchemy_repr import RepresentableBase

# Define the custom TypeDecorator for handling Decimals in SQLite
class SqliteDecimal(TypeDecorator):
    impl = Integer  # Store as an integer in SQLite

    def __init__(self, scale):
        TypeDecorator.__init__(self)
        self.scale = scale
        self.multiplier_int = 10 ** self.scale

    def process_bind_param(self, value, dialect):
        if value is not None:
            # Convert Decimal to integer for storage
            return int(Decimal(value) * self.multiplier_int)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            # Convert integer back to Decimal upon retrieval
            return Decimal(value) / self.multiplier_int
        return value


Base = declarative_base(cls=RepresentableBase)

class ColumnMap(Base):
    __tablename__ = 'column_maps'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    map_name = Column(String, unique=True, index=True)
    date_column = Column(String)
    description_column = Column(String)
    amount_column = Column(String)
    date_format = Column(String)
    negative_amounts = Column(Boolean, default=True)

class Account(Base):
    __tablename__ = 'accounts'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_path = Column(String, unique=True, index=True)
    description = Column(String)
    in_gnucash = Column(Boolean)
    balance = Column(SqliteDecimal(2), default="0.00")
    
class MatcherRule(Base):
    __tablename__ = 'matcher_rules'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    regexp = Column(String, index=True, unique=True)
    no_case = Column(Boolean)
    account_path = Column(String)
    matches = relationship("CCTransaction", backref="matcher")

    pre_compiled = None

    def __str__(self):
        return f"{self.regexp}(I={self.no_case}) -> {self.account_path}"
    
    @property
    def compiled(self):
        if not self.pre_compiled:
            if self.no_case:
                self.pre_compiled = re.compile(self.regexp, re.IGNORECASE)
            else:
                self.pre_compiled = re.compile(self.regexp)
        return self.pre_compiled


class CCTransactionFile(Base):
    __tablename__ = 'cc_transaction_files'

    id = Column(Integer, primary_key=True, autoincrement=True)
    external_id = Column(String) # typically cc name string
    import_source_file = Column(String)
    col_map_id = Column(Integer, ForeignKey("column_maps.id", ondelete="SET NULL"), nullable=True)
    transactions = relationship("CCTransaction", backref="transaction_file")



class CCTransactionsRaw(Base):
    __tablename__ = 'cc_raw_transactions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    col_names_json = Column(String)
    rows_json = Column(String)
    file_id = Column(Integer, ForeignKey("cc_transaction_files.id", ondelete="CASCADE"))

    col_names = None
    rows = None
    
    def get_col_names(self):
        if self.col_names == None:
            self.col_names = json.loads(self.col_names_json)
        return self.col_names
    
    def get_rows(self):
        if self.rows == None:
            self.rows = json.loads(self.rows_json)
        return self.rows
    
    
class CCTransaction(Base):
    __tablename__ = 'cc_transactions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    description = Column(String)
    amount = Column(SqliteDecimal(2))
    is_payment = Column(Boolean, default=False)
    file_id = Column(Integer, ForeignKey("cc_transaction_files.id", ondelete="CASCADE"))
    matcher_id = Column(Integer, ForeignKey("matcher_rules.id", ondelete="SET NULL"), nullable=True)
    

card_file_col_maps = {
    'boa': {
            "date_column": "Posted Date",
            "description_column": "Payee",
            "amount_column": "Amount",
            "date_format": "%m/%d/%Y"
            }
}

def get_account_defs(parent, acc_type, parent_string=None, leaf_only=True):
    recs = []
    for acc in parent.children:
        if acc.type == acc_type:
            if parent_string is not None:
                string = parent_string + f":{acc.name}"
            else:
                string =  acc.name
            if len(acc.children) > 0 and leaf_only:
                recs += get_account_defs(acc, acc_type, string)
                continue
            rec = dict(account_path=string,
                       description=acc.description)
            recs.append(rec)
    return recs

def extract_gnucash_accounts(gnucash_path, account_type="EXPENSE"):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa_exc.SAWarning)
        with open_book(str(gnucash_path)) as book:
            parent = book.root_account
            recs = get_account_defs(parent, account_type)
    return recs

    
    
class DataService:

    def __init__(self, ops_dir):
        self.ops_dir = Path(ops_dir)
        self.db_file = self.ops_dir / "ctrack.db"
        self.engine = create_engine(f'sqlite:///{self.db_file}')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.ensure_tables()
        self.transaction_sets = {}
        self.gnucash_path = None
        self.matcher_file_path = None

    def ensure_tables(self):
        session = self.Session()
        try:
            for name, mapping in card_file_col_maps.items():
                # Check if the mapping already exists
                if not session.query(ColumnMap).filter_by(map_name=name).first():
                    # Create new ColumnMap instance
                    new_map = ColumnMap(
                        map_name=name,
                        date_column=mapping["date_column"],
                        description_column=mapping["description_column"],
                        amount_column=mapping["amount_column"],
                        date_format=mapping["date_format"]
                    )
                    session.add(new_map)
            session.commit()
        finally:
            session.close()

    def load_gnucash_file(self, gnucash_path):
        self.gnucash_path = gnucash_path
        recs = extract_gnucash_accounts(gnucash_path)
        self.load_accounts(recs)
        
    def load_accounts(self, recs):
        session = self.Session()
        try:
            # we clear the existing data, gnucash is system of record for this
            session.query(Account).delete()
            for item in recs:
                if not session.query(Account).filter_by(account_path=item['account_path']).first():
                    rec = Account(account_path=item['account_path'], description=item['description'],
                                  in_gnucash=True)
                    session.add(rec)
            session.commit()
        finally:
            session.close()

    def accounts_count(self):
        session = self.Session()
        try:
            count = session.query(Account).count()
        finally:
            session.close()
        return count

    def get_accounts(self):
        session = self.Session()
        res = []
        try:
            for account in session.query(Account):
                res.append(account)
        finally:
            session.close()
        return res

    def get_account(self, path):
        session = self.Session(expire_on_commit=False)
        account = None
        try:
            account = session.query(Account).filter_by(account_path=path).first()
        finally:
            session.close()
        return account

    def add_account(self, account_path, description):
        session = self.Session(expire_on_commit=False)
        try:
            account = Account(account_path=account_path, description=description,
                                  in_gnucash=False)
            session.add(account)
            session.commit()
        finally:
            session.close()
        return account

    def update_gnucash_accounts(self):
        def find_account(parent, name):
            for acc in parent.children:
                if acc.name == name:
                    return acc
            return None
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            with open_book(str(self.gnucash_path), readonly=False) as book:
                USD = book.commodities.get(mnemonic="USD")
                session = self.Session()
                try:
                    for l_accnt in session.query(Account).filter_by(in_gnucash=False):
                        parent = book.root_account
                        parts = l_accnt.account_path.split(':')
                        for index, part in enumerate(parts):
                            account = find_account(parent, part)
                            if not account:
                                if index == len(parts) -1:
                                    desc = l_accnt.description
                                else:
                                    desc = ""
                                account = CASH_Account(name=part,
                                                 type="EXPENSE",
                                                 parent=parent,
                                                 commodity=USD,
                                                 description=desc)
                                print(f"Added account {account}")
                            parent = account
                    book.save()
                finally:
                    session.close()

    def load_matcher_file(self, matcher_file_path):
        self.matcher_file_path = matcher_file_path
        session = self.Session()
        try:
            with open(self.matcher_file_path) as f:
                csv_reader = csv.DictReader(f)
                for row in csv_reader:
                    re_str = row['cc_desc_re']
                    no_case = True if row['re_no_case'].lower() == "true" else False
                    account_path = row['account_path']
                    if not session.query(MatcherRule).filter_by(regexp=re_str).first():
                        rec = MatcherRule(regexp=re_str, no_case=no_case, account_path=account_path)
                        session.add(rec)
            session.commit()
        finally:
            session.close()
        
    def matchers_count(self):
        session = self.Session()
        try:
            count = session.query(MatcherRule).count()
        finally:
            session.close()
        return count
    
    def get_matchers(self):
        session = self.Session(expire_on_commit=False)
        res = []
        try:
            for matcher in session.query(MatcherRule):
                res.append(matcher)
        finally:
            session.close()
        return res

    def add_matcher(self, regexp, no_case, account_path):
        session = self.Session(expire_on_commit=False)
        try:
            rec = MatcherRule(regexp=regexp, no_case=no_case, account_path=account_path)
            session.add(rec)
            session.commit()
        finally:
            session.close()
        return rec
    
    def add_unmapped_transaction_file(self, csv_path, external_id="UNSET"):
        rows = []
        path = Path(csv_path).resolve()
        with open(path) as f:
            csv_reader = csv.DictReader(f)
            field_names = csv_reader.fieldnames
            for row in csv_reader:
                rows.append(dict(row))
        session = self.Session(expire_on_commit=False)
        try:
            orig_file_rec = session.query(CCTransactionFile).filter_by(import_source_file=str(path)).first()
            if orig_file_rec is not None:
                # we coule be reloading the file, which is legal but implies
                # removing any previous data, cascade should do it
                session.delete(orig_file_rec)
            file_rec = CCTransactionFile(external_id=external_id,
                                         import_source_file=str(path))
            session.add(file_rec)
            session.commit()

            found_matcher_id = None
            ctran = CCTransactionsRaw(col_names_json=json.dumps(field_names),
                                      rows_json=json.dumps(rows),
                                      file_id=file_rec.id)
            session.add(ctran)
            session.commit()
        finally:
            session.close()
        return file_rec

    def reload_transactions(self, csv_path, external_id="UNSET"):
        session = self.Session(expire_on_commit=False)
        try:
            file_rec = session.query(CCTransactionFile).filter_by(import_source_file=str(csv_path)).delete()
            session.commit()
        finally:
            session.close()
        return self.load_transactions(csv_path, external_id)
        
    def load_transactions(self, csv_path, external_id="UNSET"):
        file_rec = self.add_unmapped_transaction_file(csv_path, external_id)
        session = self.Session(expire_on_commit=False)
        try:
            raw = session.query(CCTransactionsRaw).filter_by(file_id=file_rec.id).first()
            use_col_map = None
            columns = raw.get_col_names()
            for col_map in session.query(ColumnMap):
                if (col_map.date_column in columns
                    and col_map.description_column in columns
                    and col_map.amount_column in columns):
                    file_rec.col_map_id = col_map.id
                    use_col_map = col_map
                    session.add(file_rec)
            if file_rec.col_map_id is None:
                return file_rec
            rows = raw.get_rows()
            matchers = self.get_matchers()
            for row in rows:
                desc_raw = row[use_col_map.description_column]
                date = datetime.strptime(row[use_col_map.date_column], use_col_map.date_format)
                amount = Decimal(row[use_col_map.amount_column])
                is_payment = True if amount > 0 else False
                found_matcher_id = None
                for matcher in matchers:
                    if matcher.compiled.match(desc_raw):
                        found_matcher_id = matcher.id
                        break
                cctr = CCTransaction(date=date,
                                     description=row[use_col_map.description_column],
                                     amount=amount,
                                     is_payment=is_payment,
                                     file_id=file_rec.id,
                                     matcher_id=found_matcher_id)
                session.add(cctr)
            session.commit()
        finally:
            session.close()

        return file_rec
                        
    def get_transactions(self, transactions_file):
        session = self.Session(expire_on_commit=False)
        res = []
        try:
            for xact in session.query(CCTransaction).filter_by(file_id=transactions_file.id):
                res.append(xact)
        finally:
            session.close()
        return res

    def update_transaction_matcher(self, xaction):
        session = self.Session(expire_on_commit=False)
        res = []
        try:
            xact = session.query(CCTransaction).filter_by(id=xaction.id).first()
            xact.matcher_id = xaction.matcher_id
            session.commit()
        finally:
            session.close()
        return xact

    def standardize_transactions(self, file_rec, output_path=None,
                                 include_payments=False,
                                 payments_account=None):
        recs = self.get_transactions(file_rec)
        session = self.Session()
        rows = []
        try:
            for rec in recs:
                if rec.matcher_id is None:
                    if not rec.is_payment:
                        raise Exception(f"cannot standardize file "
                                        f"{file_rec.import_source_file}, unmatched desc {rec.description}")
                    if not include_payments:
                        continue
                    account_path = ""
                else:
                    matcher = session.query(MatcherRule).filter_by(id=rec.matcher_id).first()
                    account_path = matcher.account_path
                rows.append({
                    'Date': rec.date,
                    'Description': rec.description,
                    'Amount': rec.amount,
                    'GnucashAccount': account_path,
                    })
        finally:
            session.close()
        if output_path is not None:
            with open(output_path, 'w') as f:
                fieldnames = ["Date", "Description", "Amount", "GnucashAccount"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    if row['GnucashAccount'] == '':
                        if include_payments:
                            if payments_account is None:
                                raise Exception('if including payments, payments_account must be provided')
                            row['GnucashAccount'] = payments_account
                    writer.writerow(row)
        return rows

    def do_cc_transactions(self, file_rec, cc_account_path,
                          include_payments=False, payments_account_path=None):


        def do_charge(rec, session, book, cc_account, expense_account):
            usd = book.commodities(mnemonic="USD")
            Transaction(
                currency=usd,
                post_date=rec.date,
                description=rec.description,
                splits=[
                    Split(account=cc_account, value=rec.amount),
                    Split(account=expense_account, value=-rec.amount)
                ]
            )
            
        def do_payment(rec, session, book_session, cc_account, payments_account):
            usd = book.commodities(mnemonic="USD")
            Transaction(
                currency=usd,
                post_date=rec.date,
                description="Payment",
                splits=[
                    Split(account=payments_account, value=-rec.amount),
                    Split(account=cc_account, value=rec.amount)
                ]
            )

        result_balances = {}
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            with open_book(str(self.gnucash_path), readonly=False) as book:
                cc_account = book.accounts(fullname=cc_account_path)
                recs = self.get_transactions(file_rec)
                session = self.Session()
                if include_payments:
                    if payments_account_path is None:
                        raise Exception(f"Must supply payments_account_path")
                    payments_account = book.accounts(fullname=payments_account_path)
                try:
                    for rec in recs:
                        if rec.is_payment:
                            if include_payments:
                                do_payment(rec, session, book, cc_account, payments_account)
                            continue
                        matcher = session.query(MatcherRule).filter_by(id=rec.matcher_id).first()
                        t_account_path = matcher.account_path
                        t_account = book.accounts(fullname=matcher.account_path)
                        do_charge(rec, session, book, cc_account, t_account)
                        result_balances[t_account_path] = t_account.get_balance()
                    result_balances[cc_account_path] = cc_account.get_balance()
                    result_balances[payments_account_path] = payments_account.get_balance()
                finally:
                    session.close()
                    book.save()
                
        return result_balances
    
    def get_column_maps(self):
        session = self.Session(expire_on_commit=False)
        res = []
        try:
            for cmap in session.query(ColumnMap):
                res.append(cmap)
        finally:
            session.close()
        return res

    def add_column_map(self, map_name, date_column, description_column,
                       amount_column, date_format):
        
        session = self.Session(expire_on_commit=False)
        try:
            rec = ColumnMap(map_name=map_name,
                            date_column=date_column,
                            description_column=description_column,
                            amount_column=amount_column,
                            date_format=date_format)
            session.add(rec)
            session.commit()
        finally:
            session.close()
        return rec
