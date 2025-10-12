from pathlib import Path
import csv
import re
import json
import datetime 
from decimal import Decimal
from typing import Optional
from dataclasses import dataclass, field
from decimal import Decimal
import warnings

from piecash import open_book, create_book, Account
from sqlalchemy.types import TypeDecorator
from sqlalchemy import create_engine, Column, ForeignKey
from sqlalchemy import Boolean, Integer, Date, String, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import exc as sa_exc

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


Base = declarative_base()

class ColumnMap(Base):
    __tablename__ = 'column_maps'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    map_name = Column(String, unique=True, index=True)
    date_col_name = Column(String)
    desc_col_name = Column(String)
    amt_col_name = Column(String)
    date_format = Column(String)

class Account(Base):
    __tablename__ = 'accounts'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_path = Column(String, unique=True, index=True)
    description = Column(String)
    in_gnucash = Column(Boolean)

    @property
    def path_tail(self):
        return ':'.join(self.account_path.split(':')[1:])
    
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
    prepared_file = Column(String, nullable=True)
    col_map_id = Column(Integer, ForeignKey("column_maps.id", ondelete="SET NULL"), nullable=True)

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
    file_id = Column(Integer, ForeignKey("cc_transaction_files.id", ondelete="CASCADE"))
    matcher_id = Column(Integer, ForeignKey("matcher_rules.id", ondelete="SET NULL"), nullable=True)
    
    
@dataclass
class TransactionRow:
    raw: str
    date: Optional[datetime.date] = None
    description: Optional[str] = None
    amount: Optional[Decimal] = None
    matcher: Optional[MatcherRule] = None
    
    
@dataclass
class TransactionSet:
    load_path: str
    column_names: list[str] = field(default_factory=list)
    rows: list[TransactionRow] = field(default_factory=list)
    column_map: Optional[ColumnMap] = None
    date_index: int = None
    desc_index: int = None
    amt_index: int = None
    
    
@dataclass
class TransactionFileSpec:
    file_path: str
    col_map: ColumnMap
    transactions: TransactionSet



card_file_col_maps = {
    'boa': {
            "date_col_name": "Posted Date",
            "desc_col_name": "Payee",
            "amt_col_name": "Amount",
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
        self.transasction_files: list[TransactionFileSpec] = []
        self.unmapped_t_files = []

    def ensure_tables(self):
        session = self.Session()
        try:
            for name, mapping in card_file_col_maps.items():
                # Check if the mapping already exists
                if not session.query(ColumnMap).filter_by(map_name=name).first():
                    # Create new ColumnMap instance
                    new_map = ColumnMap(
                        map_name=name,
                        date_col_name=mapping["date_col_name"],
                        desc_col_name=mapping["desc_col_name"],
                        amt_col_name=mapping["amt_col_name"],
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
        session = self.Session()
        account = None
        try:
            account = session.query(Account).filter_by(account_path=path).first()
        finally:
            session.close()
        return account

    def save_account(self, account):
        session = self.Session()
        try:
            session.add(account)
            session.commit()
        finally:
            session.close()
        return account

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
        
    def load_matchers(self, matchers):
        session = self.Session()
        try:
            for matcher in matchers:
                if not session.query(MatcherRule).filter_by(regexp=matcher.re_str).first():
                    rec = MatcherRule(regexp=matcher.re_str, no_case=matcher.no_case, account_path=matcher.value)
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
        session = self.Session()
        res = []
        try:
            for matcher in session.query(MatcherRule):
                res.append(matcher)
        finally:
            session.close()
        return res

    def save_matcher_rule(self, matcher_rule):
        session = self.Session()
        res = []
        try:
            session.add(matcher_rule)
            session.commit()
        finally:
            session.close()
        return res

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
            file_rec = session.query(CCTransactionFile).filter_by(import_source_file=str(path)).first()
            if file_rec is not None:
                # we coule be reloading the file, which is legal but implies
                # removing any previous data, cascade should do it
                session.delete(file_rec)
            else:
                file_rec = CCTransactionFile(external_id=external_id,
                                             import_source_file=str(path))
                session.add(file_rec)
                session.commit()
            ctran = CCTransactionsRaw(col_names_json=json.dumps(field_names),
                                      rows_json=json.dumps(rows),
                                      file_id=file_rec.id)
            session.add(ctran)
            session.commit()
        finally:
            session.close()
        return file_rec
        
    def load_transactions(self, csv_path, external_id="UNSET"):
        file_rec = self.add_unmapped_transaction_file(csv_path, external_id)
        session = self.Session()
        try:
            raw = session.query(CCTransactionsRaw).filter_by(file_id=file_rec.id).first()
            use_col_map = None
            if file_rec.col_map_id is None:
                column_names = raw.get_col_names()
                for col_map in session.query(ColumnMap):
                    if (col_map.date_col_name in column_names
                        and col_map.desc_col_name in column_names
                        and col_map.amt_col_name in column_names):
                        file_rec.col_map_id = col_map.id
                        use_col_map = col_map
                        session.add(file_rec)
            else:
                use_col_map = session.query(ColumnMap).filter_by(id=file_rec.col_map_id).first()
            if file_rec.col_map_id is None:
                return file_rec
            rows = raw.get_rows()
            matchers = self.get_matchers()
            for row in rows:
                found_matcher_id = None
                for matcher in matchers:
                    desc_raw = row[use_col_map.desc_col_name]
                    if matcher.compiled.match(desc_raw):
                        found_matcher_id = matcher.id
                        break
                cctr = CCTransaction(date=row[use_col_map.date_col_name],
                                     description=row[use_col_map.desc_col_name],
                                     amount=row[use_col_map.amt_col_name],
                                     file_id=file_rec.id,
                                     matcher_id=found_matcher_id)
                session.add(cctr)
        finally:
            session.close()

        return file_rec
        
    def oldload_transactions(self, csv_path):
        rows = []
        with open(csv_path) as f:
            csv_reader = csv.reader(f)
            field_names = None
            for index,row in enumerate(csv_reader):
                if index == 0:
                    field_names = row
                    continue
                rows.append(TransactionRow(raw=row))
        self.transaction_sets[csv_path] = tset = TransactionSet(load_path=csv_path,
                                                                column_names=field_names,
                                                                rows=rows)
        from pprint import pprint
        if tset.column_map is None:
            # try to find importable columns
            session = self.Session()
            pprint(tset.column_names)
            try:
                for col_map in session.query(ColumnMap):
                    pprint(col_map.__dict__)
                    if (col_map.date_col_name in tset.column_names
                        and col_map.desc_col_name in tset.column_names
                        and col_map.amt_col_name in tset.column_names):
                        tset.column_map = col_map
                        print("match")
                        for index, name in enumerate(tset.column_names):
                            if name == col_map.date_col_name:
                                tset.date_index = index
                            elif name == col_map.desc_col_name:
                                tset.desc_index = index
                            if name == col_map.amt_col_name:
                                tset.amt_index = index
                        for row in tset.rows:
                            row.date = datetime.datetime.strptime(row.raw[tset.date_index],
                                                                  tset.column_map.date_format)
                            row.description = row.raw[tset.desc_index]
                            row.amount = Decimal(row.raw[tset.amt_index])
                                
            finally:
                session.close()
        if tset.column_map is None:
            return None
        matchers = self.get_matchers()
        for row in tset.rows:
            if row.matcher is None:
                for matcher in matchers:
                    desc_raw = row.raw[tset.desc_index]
                    if matcher.compiled.match(desc_raw):
                        row.matcher = matcher
        return TransactionFileSpec(csv_path, tset.column_map, tset)
                        
    def transaction_sets_stats(self):
        set_count = len(self.transaction_sets)
        trows = 0
        for p,ts in self.transaction_sets.items():
            trows += len(ts.rows)
        return set_count, trows

    def get_transaction_sets(self):
        return self.transaction_sets

    def get_transaction_set(self, file_path):
        return self.transaction_sets[str(file_path)]
