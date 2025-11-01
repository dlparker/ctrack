from pathlib import Path
from enum import StrEnum, auto
from ctrack.data_service import DataService



class DataNeeded(StrEnum):
    GNUCASH = auto()
    XACTION_FILE = auto()
    COLUMN_MAP = auto()
    MATCHER_RULE = auto()
    ACCOUNT = auto()
    ACCOUNT_SYNC = auto()
    
class NextStep(StrEnum):
    SET_GNUCASH = auto()
    LOAD_XACTION_FILE = auto()
    ADD_COLUMN_MAP = auto()
    ADD_MATCHER_RULE = auto()
    ADD_ACCOUNT = auto()
    DO_ACCOUNT_SYNC = auto()
    SAVE_XACTIONS = auto()
    
class MainFlow:

    def __init__(self, data_dir, gnucash_path=None):
        self.data_dir = Path(data_dir)
        self.dataservice = DataService(self.data_dir)
        if gnucash_path is not None:
            self.data_service.load_gnucash_file(gnucash_path)
        self.gnucash_path = self.dataservice.gnucash_path
        self.xaction_files = []

    def get_next_step(self):
        needs = self.get_data_needs()
        if DataNeeded.GNUCASH in needs:
            return NextStep.SET_GNUCASH
        if DataNeeded.XACTION_FILE in needs:
            return NextStep.LOAD_XACTION_FILE
        if DataNeeded.COLUMN_MAP in needs:
            return NextStep.ADD_COLUMN_MAP
        if DataNeeded.MATCHER_RULE in needs:
            return NextStep.ADD_MATCHER_RULE
        if DataNeeded.ACCOUNT in needs:
            return NextStep.ADD_ACCOUNT
        if DataNeeded.ACCOUNT_SYNC in needs:
            return NextStep.DO_ACCOUNT_SYNC
        for xfile in self.xaction_files:
            if xfile.saved_to_gnucash is False:
                return NextStep.SAVE_XACTIONS
        return NextStep.LOAD_XACTION_FILE
        
    def set_gnucash(self, path):
        self.gnucash_path = path
        self.dataservice.set_gnucash_file(self.gnucash_path)

    def add_xaction_file(self, path):
        file_rec = self.dataservice.load_transactions(path)
        self.xaction_files.append(file_rec)

    def add_column_map(self, name, date_col, desc_col, amount_col, date_format):
        self.dataservice.add_column_map(name, date_col, desc_col, amount_col, date_format)

        for xfile in self.xaction_files:
            new_files_list = []
            if not xfile.columns_mapped:
                xfile = self.dataservice.reload_transactions(xfile.import_source_file)
            new_files_list.append(xfile)
        self.xaction_files = new_files_list

    def add_matcher_rule(self, regexp, no_case, account_name):
        self.dataservice.add_matcher(regexp, no_case, account_name)
        for xfile in self.xaction_files:
            new_files_list = []
            matched, unmatched = xfile.rows_matched(self.dataservice)
            if unmatched > 0:
                xfile = self.dataservice.reload_transactions(xfile.import_source_file)
            new_files_list.append(xfile)
        self.xaction_files = new_files_list

    def add_account(self, name, description, save=False):
        account = self.dataservice.add_account(name, description)
        if save:
            self.dataservice.save_account(name)
        return account

    def load_matcher_rules_file(self, path):
        self.dataservice.load_matcher_file(path)
        for xfile in self.xaction_files:
            new_files_list = []
            matched, unmatched = xfile.rows_matched(self.dataservice)
            if unmatched > 0:
                xfile = self.dataservice.reload_transactions(xfile.import_source_file)
            new_files_list.append(xfile)
        self.xaction_files = new_files_list
        
    def get_data_needs(self):
        res = set()
        if self.gnucash_path is None:
            res.add(DataNeeded.GNUCASH)
        if len(self.xaction_files) == 0:
            res.add(DataNeeded.XACTION_FILE)
        else:
            for x_file in self.xaction_files:
                if not x_file.columns_mapped:
                    res.add(DataNeeded.COLUMN_MAP)
                matched, unmatched = x_file.rows_matched(self.dataservice)
                if unmatched > 0:
                    res.add(DataNeeded.MATCHER_RULE)
        for rule in self.dataservice.get_matchers():
            has_accnt, accnt_saved = rule.account_status(self.dataservice)
            if not has_accnt:
                res.add(DataNeeded.ACCOUNT)
            elif not accnt_saved:
                res.add(DataNeeded.ACCOUNT_SYNC)
        return list(res)      

    def get_unfinished_xactions(self):
        unmapped = []
        unmatched = []
        for x_file in self.xaction_files:
            if not x_file.columns_mapped:
                unmapped.append(x_file)
            matched, no_match = x_file.rows_matched(self.dataservice)
            if no_match > 0:
                unmatched.append(x_file)
        return unmapped, unmatched

    def get_savable_xactions(self):
        res = []
        for x_file in self.xaction_files:
             if x_file.is_save_ready(self.dataservice):
                 res.append(x_file)
        return res
    
    def get_missing_accounts(self):
        missing = []
        unsaved = []
        for rule in self.dataservice.get_matchers():
            has_accnt, accnt_saved = rule.account_status(self.dataservice)
            if not has_accnt:
                missing.append(rule.account_name)
            elif not accnt_saved:
                unsaved.append(rule.account_name)
        return missing, unsaved
    
