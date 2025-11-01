import platform
import argparse
from pathlib import Path
from typing import Optional

from nicegui import events, ui
from ctrack.data_service import DataService


def main(data_dir, web_mode, gnucash_path):
    if data_dir is None:
        raise Exception('no picker for datadir yet')

    ui_app = None
    @ui.page('/')
    async def index():
        nonlocal ui_app
        from ctrack.ng_main import UIApp, MainWindow
        if ui_app is None:
            ui_app = UIApp(data_dir, gnucash_path)
            await ui_app.start()
        
    ui.run()        

if __name__ in {"__main__", "__mp_main__"}:
    print("Starting CCImport ...")
    print("Opening web browser at http://localhost:8080")
    print("Press Ctrl+C to stop the server")
    print()

    parser = argparse.ArgumentParser(description='Credit Card Importer')

    parser.add_argument('-w', '--web-page', action='store_true',
                       help="Run as a web server and open page in browser")
    parser.add_argument('-d', '--data-dir', type=str, default=None,
                       help="Path to data directory for database and working files")
    parser.add_argument('-g', '--gnucash', type=str, default=None,
                       help="Path to GnuCash database file")

    args = parser.parse_args()
    main(data_dir=args.data_dir,
         web_mode=args.web_page,
         gnucash_path=args.gnucash
         )
