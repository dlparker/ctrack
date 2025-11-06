from __future__ import annotations   # <-- added for forward reference
from pathlib import Path
import csv
import re
import json
import datetime 
from decimal import Decimal
from typing import Optional
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
from sqlalchemy.orm import object_session
from sqlalchemy import exc as sa_exc
from sqlalchemy import event
from sqlalchemy_repr import RepresentableBase

# ----------------------------------------------------------------------
# Custom Decimal handling for SQLite
# ----------------------------------------------------------------------
class SqliteDecimal(TypeDecorator):
    impl = Integer

    def __init__(self, scale):
        super().__init__()
        self.scale = scale
        self.multiplier_int = 10 ** self.scale

    def process_bind_param(self, value, dialect):
        if value is not None:
            return int(Decimal(value) * self.multiplier_int)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            return Decimal(value) / self.multiplier_int
        return value


Base = declarative_base(cls=RepresentableBase)

# ----------------------------------------------------------------------
# ORM models
# ----------------------------------------------------------------------
class MetaData(Base):
    __tablename__ = 'meta_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    gnucash_file = Column(String)

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
    name = Column(String, unique=True, index=True)
    description = Column(String)
    in_gnucash = Column(Boolean)
    balance = Column(SqliteDecimal(2), default="0.00")

class MatcherRule(Base):
    __tablename__ = 'matcher_rules'
    id = Column(Integer, primary_key=True, autoincrement=True)
    regexp = Column(String, index=True, unique=True)
    no_case = Column(Boolean)
    account_name = Column(String)
    matches = relationship("CCTransaction", backref="matcher")

    pre_compiled = None

    def __str__(self):
        return f"{self.regexp}(I={self.no_case}) -> {self.account_name}"

    @property
    def compiled(self):
        if not self.pre_compiled:
            flags = re.IGNORECASE if self.no_case else 0
            self.pre_compiled = re.compile(self.regexp, flags)
        return self.pre_compiled

    def account_status(self, dataservice):
        accnt = dataservice.get_account(self.account_name)
        if accnt is None:
            return False, False
        return True, accnt.in_gnucash


class CCTransactionFile(Base):
    __tablename__ = 'cc_transaction_files'

    id = Column(Integer, primary_key=True, autoincrement=True)
    external_id = Column(String)               # typically cc name string
    import_source_file = Column(String)
    column_map_id = Column(Integer, ForeignKey("column_maps.id", ondelete="SET NULL"), nullable=True)
    saved_to_gnucash = Column(Boolean, default=False)
    transactions = relationship("CCTransaction", backref="transaction_file")

    # ------------------------------------------------------------------
    # Non-persistent reference to the owning DataService
    # ------------------------------------------------------------------
    _dataservice: Optional["DataService"] = None   # <-- plain attribute

    @property
    def columns_mapped(self):
        return self.column_map_id is not None

    @property
    def display_name(self):
        return f"ID-{self.id}:{Path(self.import_source_file).parts[-1]}"

    def rows_matched(self):
        matched = unmatched = 0
        for xact in self._dataservice.get_transactions(self):
            if xact.matcher_id is not None:
                matched += 1
            else:
                unmatched += 1
        return matched, unmatched

    def is_save_ready(self):
        for xact in self._dataservice.get_transactions(self):
            if xact.matcher_id is None:
                return False
            matcher = self._dataservice.get_matcher_by_id(xact.matcher_id)
            account = self._dataservice.get_account(matcher.account_name)
            if account is None or not account.in_gnucash:
                return False
        return True

    def save_to_gnucash(self, cc_account_name, payments_account_name):
        if not self.is_save_ready():
            raise Exception(f'cannot save file {self.import_source_file}, not ready')
        self._dataservice.do_cc_transactions(self, cc_account_name, True, payments_account_name)

    def get_raw_data(self):
        session = self._dataservice.Session(expire_on_commit=False)
        try:
            res = session.query(CCTransactionsRaw).filter_by(file_id=self.id).first()
        finally:
            session.close()
        return res

    def get_column_map(self):
        if self.column_map_id is None:
            return None
        session = self._dataservice.Session(expire_on_commit=False)
        try:
            res = session.query(ColumnMap).filter_by(id=self.column_map_id).first()
        finally:
            session.close()
        return res

@event.listens_for(CCTransactionFile, "load")
def _inject_dataservice_on_load(target, context):
    """
    Automatically attach the owning DataService to every CCTransactionFile
    when it is loaded from the database.
    """
    session = object_session(target)
    if session is None:
        return

    # Find the DataService that owns this session.
    # All DataService instances use sessionmaker(bind=engine), so we can
    # walk up from the session to find the DataService via its .bind
    engine = session.get_bind()
    # In practice, only one DataService exists per process, so we keep a weak ref registry:
    if not hasattr(engine, "_dataservice_owner"):
        return
    dataservice = engine._dataservice_owner()
    if dataservice:
        target._dataservice = dataservice


class CCTransactionsRaw(Base):
    __tablename__ = 'cc_raw_transactions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    col_names_json = Column(String)
    rows_json = Column(String)
    file_id = Column(Integer, ForeignKey("cc_transaction_files.id", ondelete="CASCADE"))

    col_names = None
    rows = None

    def get_col_names(self):
        if self.col_names is None:
            self.col_names = json.loads(self.col_names_json)
        return self.col_names

    def get_rows(self):
        if self.rows is None:
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
    raw_row_number = Column(Integer)

# ----------------------------------------------------------------------
# Hard-coded column maps (e.g. for Bank of America)
# ----------------------------------------------------------------------
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
            string = f"{parent_string}:{acc.name}" if parent_string else acc.name
            if len(acc.children) > 0 and leaf_only:
                recs += get_account_defs(acc, acc_type, string)
                continue
            recs.append(dict(name=string, description=acc.description))
    return recs


def extract_gnucash_accounts(gnucash_path, account_type="EXPENSE"):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa_exc.SAWarning)
        with open_book(str(gnucash_path)) as book:
            return get_account_defs(book.root_account, account_type)

class DataService:
    def __init__(self, ops_dir):
        self.ops_dir = Path(ops_dir)
        self.db_file = self.ops_dir / "ctrack.db"
        self.engine = create_engine(f'sqlite:///{self.db_file}')
        # ---- NEW: register this DataService as the owner of the engine ----
        import weakref
        self.engine._dataservice_owner = weakref.ref(self)
        # -----------------------------------------------------------------
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.transaction_sets = {}
        self.gnucash_path = None
        self.matcher_file_path = None
        self.ensure_tables()

    # ------------------------------------------------------------------
    # Table initialisation / meta handling
    # ------------------------------------------------------------------
    def ensure_tables(self):
        session = self.Session()
        try:
            for name, mapping in card_file_col_maps.items():
                if not session.query(ColumnMap).filter_by(map_name=name).first():
                    session.add(ColumnMap(
                        map_name=name,
                        date_column=mapping["date_column"],
                        description_column=mapping["description_column"],
                        amount_column=mapping["amount_column"],
                        date_format=mapping["date_format"]
                    ))
            session.commit()

            meta = session.query(MetaData).first()
            if meta:
                self.gnucash_path = meta.gnucash_file
                print('found file')
        finally:
            session.close()

    # ------------------------------------------------------------------
    # ColumnMap helpers
    # ------------------------------------------------------------------
    def get_column_maps(self):
        session = self.Session(expire_on_commit=False)
        try:
            return list(session.query(ColumnMap))
        finally:
            session.close()

    def get_column_map(self, map_id):
        session = self.Session(expire_on_commit=False)
        try:
            return session.query(ColumnMap).filter(ColumnMap.id == map_id).first()
        finally:
            session.close()

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
            return rec
        finally:
            session.close()

    # ------------------------------------------------------------------
    # GnuCash file handling
    # ------------------------------------------------------------------
    def set_gnucash_file(self, gnucash_path):
        session = self.Session()
        try:
            meta = session.query(MetaData).first()
            if meta is None:
                meta = MetaData(gnucash_file=str(gnucash_path))
                session.add(meta)
                session.commit()
            elif Path(meta.gnucash_file).resolve() != Path(gnucash_path).resolve():
                raise Exception('cannot change gnucash file')
        finally:
            session.close()

        self.gnucash_path = gnucash_path
        self.load_accounts(extract_gnucash_accounts(gnucash_path))

    def load_gnucash_file(self):
        self.load_accounts(extract_gnucash_accounts(self.gnucash_path))

    def load_accounts(self, recs):
        session = self.Session()
        try:
            session.query(Account).delete()
            for item in recs:
                if not session.query(Account).filter_by(name=item['name']).first():
                    session.add(Account(name=item['name'],
                                        description=item['description'],
                                        in_gnucash=True))
            session.commit()
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Account helpers
    # ------------------------------------------------------------------
    def accounts_count(self):
        session = self.Session()
        try:
            return session.query(Account).count()
        finally:
            session.close()

    def get_accounts(self):
        session = self.Session()
        try:
            return list(session.query(Account))
        finally:
            session.close()

    def get_account(self, path):
        session = self.Session(expire_on_commit=False)
        try:
            return session.query(Account).filter_by(name=path).first()
        finally:
            session.close()

    def add_account(self, name, description):
        session = self.Session(expire_on_commit=False)
        try:
            acct = Account(name=name, description=description, in_gnucash=False)
            session.add(acct)
            session.commit()
            return acct
        finally:
            session.close()

    def save_account(self, name):
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
                    l_accnt = session.query(Account).filter_by(name=name).first()
                    parent = book.root_account
                    parts = l_accnt.name.split(':')
                    for idx, part in enumerate(parts):
                        account = find_account(parent, part)
                        if not account:
                            desc = l_accnt.description if idx == len(parts) - 1 else ""
                            account = CASH_Account(name=part,
                                                   type="EXPENSE",
                                                   parent=parent,
                                                   commodity=USD,
                                                   description=desc)
                        parent = account
                    book.save()
                    l_accnt.in_gnucash = True
                    session.commit()
                finally:
                    session.close()

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
                        parts = l_accnt.name.split(':')
                        for idx, part in enumerate(parts):
                            account = find_account(parent, part)
                            if not account:
                                desc = l_accnt.description if idx == len(parts) - 1 else ""
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

    # ------------------------------------------------------------------
    # Matcher helpers
    # ------------------------------------------------------------------
    def load_matcher_file(self, matcher_file_path):
        self.matcher_file_path = matcher_file_path
        session = self.Session()
        try:
            with open(matcher_file_path) as f:
                for row in csv.DictReader(f):
                    re_str = row['cc_desc_re']
                    no_case = row['re_no_case'].lower() == "true"
                    name = row['account_path']
                    if not session.query(MatcherRule).filter_by(regexp=re_str).first():
                        session.add(MatcherRule(regexp=re_str,
                                                no_case=no_case,
                                                account_name=name))
            session.commit()
        finally:
            session.close()

    def matchers_count(self):
        session = self.Session()
        try:
            return session.query(MatcherRule).count()
        finally:
            session.close()

    def get_matchers(self):
        session = self.Session(expire_on_commit=False)
        try:
            return list(session.query(MatcherRule))
        finally:
            session.close()

    def get_matcher_by_id(self, mid):
        session = self.Session(expire_on_commit=False)
        try:
            return session.query(MatcherRule).filter_by(id=mid).first()
        finally:
            session.close()

    def add_matcher(self, regexp, no_case, name):
        session = self.Session(expire_on_commit=False)
        try:
            rec = MatcherRule(regexp=regexp, no_case=no_case, account_name=name)
            session.add(rec)
            session.commit()
            return rec
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Transaction file import
    # ------------------------------------------------------------------
    def add_unmapped_transaction_file(self, csv_path, external_id="UNSET"):
        path = Path(csv_path).resolve()
        with open(path) as f:
            reader = csv.DictReader(f)
            field_names = reader.fieldnames
            rows = [dict(r) for r in reader]

        session = self.Session(expire_on_commit=False)
        try:
            # Remove any previous import of the same file
            old = session.query(CCTransactionFile).filter_by(import_source_file=str(path)).first()
            if old:
                session.delete(old)

            file_rec = CCTransactionFile(external_id=external_id,
                                         import_source_file=str(path))
            file_rec._dataservice = self                     # <-- wire the service
            session.add(file_rec)
            session.commit()

            raw = CCTransactionsRaw(col_names_json=json.dumps(field_names),
                                    rows_json=json.dumps(rows),
                                    file_id=file_rec.id)
            session.add(raw)
            session.commit()
        finally:
            session.close()
        return file_rec

    def reload_transactions(self, csv_path, external_id="UNSET"):
        session = self.Session()
        try:
            rec = session.query(CCTransactionFile).filter_by(import_source_file=str(csv_path)).first()
            if rec:
                session.delete(rec)
            session.commit()
        finally:
            session.close()
        return self.load_transactions(csv_path, external_id)

    def load_transactions(self, csv_path, external_id="UNSET"):
        file_rec = self.add_unmapped_transaction_file(csv_path, external_id)
        session = self.Session(expire_on_commit=False)
        try:
            raw = session.query(CCTransactionsRaw).filter_by(file_id=file_rec.id).first()
            columns = raw.get_col_names()
            # Auto-detect column map
            for cmap in session.query(ColumnMap):
                if (cmap.date_column in columns and
                    cmap.description_column in columns and
                    cmap.amount_column in columns):
                    file_rec.column_map_id = cmap.id
                    session.add(file_rec)
                    break
            else:
                return file_rec                               # no map → stop here

            rows = raw.get_rows()
            matchers = self.get_matchers()

            for index,row in enumerate(rows):
                desc = row[cmap.description_column]
                date = datetime.strptime(row[cmap.date_column], cmap.date_format)
                amount = Decimal(row[cmap.amount_column])
                is_payment = amount > 0

                matcher_id = None
                for m in matchers:
                    if m.compiled.match(desc):
                        matcher_id = m.id
                        break

                session.add(CCTransaction(date=date,
                                          description=desc,
                                          amount=amount,
                                          is_payment=is_payment,
                                          file_id=file_rec.id,
                                          matcher_id=matcher_id,
                                          raw_row_number=index))
            session.commit()
        finally:
            session.close()
        return file_rec

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def get_transaction_files(self, unsaved_only=False, saved_only=False):
        session = self.Session(expire_on_commit=False)
        try:
            q = session.query(CCTransactionFile)
            if unsaved_only:
                q = q.filter_by(saved_to_gnucash=False)
            elif saved_only:
                q = q.filter_by(saved_to_gnucash=True)
            return list(q)
        finally:
            session.close()

    def get_transactions(self, transaction_file):
        session = self.Session(expire_on_commit=False)
        try:
            return list(session.query(CCTransaction).filter_by(file_id=transaction_file.id))
        finally:
            session.close()

    def update_transaction_matcher(self, xaction):
        session = self.Session(expire_on_commit=False)
        try:
            tx = session.query(CCTransaction).filter_by(id=xaction.id).first()
            tx.matcher_id = xaction.matcher_id
            session.commit()
            return tx
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Standardisation / export
    # ------------------------------------------------------------------
    def standardize_transactions(self, file_rec, output_path=None,
                                 include_payments=False, payments_account=None):
        recs = self.get_transactions(file_rec)
        session = self.Session()
        rows = []
        try:
            for rec in recs:
                if rec.matcher_id is None:
                    if not rec.is_payment:
                        raise Exception(f"cannot standardize file {file_rec.import_source_file}, "
                                        f"unmatched desc {rec.description}")
                    if not include_payments:
                        continue
                    acct = ""
                else:
                    matcher = session.query(MatcherRule).filter_by(id=rec.matcher_id).first()
                    acct = matcher.account_name

                rows.append({
                    'Date': rec.date,
                    'Description': rec.description,
                    'Amount': rec.amount,
                    'GnucashAccount': acct,
                })
        finally:
            session.close()

        if output_path:
            with open(output_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["Date", "Description", "Amount", "GnucashAccount"])
                writer.writeheader()
                for r in rows:
                    if not r['GnucashAccount'] and include_payments:
                        if payments_account is None:
                            raise Exception('payments_account required when including payments')
                        r['GnucashAccount'] = payments_account
                    writer.writerow(r)
        return rows

    # ------------------------------------------------------------------
    # Write to GnuCash
    # ------------------------------------------------------------------
    def do_cc_transactions(self, file_rec_in, cc_name,
                           include_payments=False, payments_name=None):
        def do_charge(rec, book, cc_account, expense_account):
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

        def do_payment(rec, book, cc_account, payments_account):
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

        balances = {}
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            with open_book(str(self.gnucash_path), readonly=False) as book:
                cc_account = book.accounts(fullname=cc_name)
                session = self.Session()
                try:
                    file_rec = session.query(CCTransactionFile).filter_by(id=file_rec_in.id).first()
                    recs = self.get_transactions(file_rec)

                    payments_account = None
                    if include_payments:
                        if payments_name is None:
                            raise Exception("payments_name required when include_payments=True")
                        payments_account = book.accounts(fullname=payments_name)

                    for rec in recs:
                        if rec.is_payment:
                            if include_payments:
                                do_payment(rec, book, cc_account, payments_account)
                            continue

                        matcher = session.query(MatcherRule).filter_by(id=rec.matcher_id).first()
                        exp_account = book.accounts(fullname=matcher.account_name)
                        do_charge(rec, book, cc_account, exp_account)
                        balances[matcher.account_name] = exp_account.get_balance()

                    balances[cc_name] = cc_account.get_balance()
                    if payments_account:
                        balances[payments_name] = payments_account.get_balance()

                    file_rec.saved_to_gnucash = True
                    session.commit()
                finally:
                    session.close()
                    book.save()
        return balances
