#!/usr/bin/env python
from ctrack.account_sync import export_gnucash_accounts


if __name__=="__main__":
    from pathlib import Path
    parent = Path(__file__).parent.parent
    ifile = parent / "test.gnucash"
    out_dir = parent / "main_out"
    if not out_dir.exists():
        out_dir.mkdir()
    export_gnucash_accounts(ifile, out_dir / "expense_accounts.csv")
    
