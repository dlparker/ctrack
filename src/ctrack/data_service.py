from pathlib import Path
import csv
from typing import Optional
from dataclasses import dataclass, field

from sqlalchemy import create_engine, Column, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from ctrack.cc_file_ops import card_file_col_maps

Base = declarative_base()

class ColumnMap(Base):
    __tablename__ = 'column_maps'
    
    map_name = Column(String, primary_key=True, unique=True)
    date_col_name = Column(String)
    desc_col_name = Column(String)
    amt_col_name = Column(String)
    date_format = Column(String)

class MatcherRule(Base):
    __tablename__ = 'matcher_rules'
    
    regexp = Column(String, primary_key=True)
    no_case = Column(Boolean)
    account_path = Column(String)

class NewAccount(Base):
    __tablename__ = 'new_accounts'
    
    account_path = Column(String, primary_key=True)
    description = Column(String)

class KnownAccount(Base):
    __tablename__ = 'known_accounts'
    
    account_path = Column(String, primary_key=True)
    description = Column(String)

@dataclass
class TransactionSet:
    load_path: str
    column_names: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    column_map: Optional[ColumnMap] = None
    
    
class DataService:

    def __init__(self, ops_dir):
        self.ops_dir = Path(ops_dir)
        self.db_file = self.ops_dir / "cc_import.db"
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
                if not session.query(KnownAccount).filter_by(account_path=item['account_path']).first():
                    rec = KnownAccount(account_path=item['account_path'], description=item['description'])
                    session.add(rec)
            session.commit()
        finally:
            session.close()

    def known_accounts_count(self):
        session = self.Session()
        try:
            count = session.query(KnownAccount).count()
        finally:
            session.close()
        return count

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
    
    def load_transactions(self, csv_path):
        rows = []
        with open(csv_path) as f:
            csv_reader = csv.DictReader(f)
            fieldnames = csv_reader.fieldnames
            for row in csv_reader:
                rows.append(row)
        self.transaction_sets[csv_path] = TransactionSet(load_path=csv_path,
                                                        column_names=fieldnames,
                                                        rows=rows)
    def transaction_sets_stats(self):
        set_count = len(self.transaction_sets)
        trows = 0
        for p,ts in self.transaction_sets.items():
            trows += len(ts.rows)
        return set_count, trows
