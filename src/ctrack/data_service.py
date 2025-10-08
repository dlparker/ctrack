from pathlib import Path
import csv
import re
import datetime 
from decimal import Decimal
from typing import Optional
from dataclasses import dataclass, field
from decimal import Decimal

from sqlalchemy.types import TypeDecorator
from sqlalchemy import create_engine, Column, ForeignKey
from sqlalchemy import Boolean, Integer, Date, String, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from ctrack.cc_file_ops import card_file_col_maps

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
    
    map_name = Column(String, primary_key=True, unique=True)
    date_col_name = Column(String)
    desc_col_name = Column(String)
    amt_col_name = Column(String)
    date_format = Column(String)

class Account(Base):
    __tablename__ = 'accounts'
    
    account_path = Column(String, primary_key=True)
    description = Column(String)
    in_gnucash = Column(Boolean)

    @property
    def path_tail(self):
        return ':'.join(self.account_path.split(':')[1:])
    
class MatcherRule(Base):
    __tablename__ = 'matcher_rules'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    regexp = Column(String, index=True)
    no_case = Column(Boolean)
    account_path = Column(String)
    matches = relationship("ImportableTransaction", backref="matcher")

    pre_compiled = None

    @property
    def compiled(self):
        if not self.pre_compiled:
            if self.no_case:
                self.pre_compiled = re.compile(self.regexp, re.IGNORECASE)
            else:
                self.pre_compiled = re.compile(self.regexp)
        return self.pre_compiled

class ImportableTransaction(Base):
    __tablename__ = 'transactions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    external_id = Column(String) # typically cc name string
    import_source_file = Column(String)
    date = Column(Date)
    description = Column(String)
    amount = Column(SqliteDecimal(2))  
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
    
    
    
class DataService:

    def __init__(self, ops_dir):
        self.ops_dir = Path(ops_dir)
        self.db_file = self.ops_dir / "ctrack.db"
        self.engine = create_engine(f'sqlite:///{self.db_file}')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.ensure_tables()
        self.transaction_sets = {}

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
        
    def load_transactions(self, csv_path, do_match=True):
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
        if tset.column_map is None:
            # try to find importable columns
            session = self.Session()
            try:
                for col_map in session.query(ColumnMap):
                    if (col_map.date_col_name in tset.column_names
                        and col_map.desc_col_name in tset.column_names
                        and col_map.amt_col_name in tset.column_names):
                        tset.column_map = col_map
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
        if tset.column_map is None or not do_match:
            return
        matchers = self.get_matchers()
        for row in tset.rows:
            if row.matcher is None:
                for matcher in matchers:
                    desc_raw = row.raw[tset.desc_index]
                    if matcher.compiled.match(desc_raw):
                        row.matcher = matcher
                        
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
