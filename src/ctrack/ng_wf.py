from dataclasses import dataclass
import re
from pathlib import Path
from typing import Optional, Any
from enum import StrEnum

from nicegui import ui
from nicegui.element import Element
from ctrack.data_service import MatcherRule, Account

from ctrack.ng_main import MainPanelContent, MainWindow, MainNav


# For phase 1, producing an importable transaction file translation, we need a transaction input
# file, but we might already have matchers and accounts loaded. We can offer to load new accounts
# from gnucash file and matchers from a csv file, but these would be side loops.
# 
# So if we have nothing then we offer the user a "load CC transactions file" button.
# 
phase_pages = {
    'NoInput': "no gnucash, matcher_map or csv defined"
    }

class WFPhaseCode(StrEnum):
    NO_INPUT = "no_input"
    GREEN = "green"
    BLUE = "blue"   

@dataclass
class AccountStatus:
    new_accounts: Optional[list[Account]] = None
    

class FirstPage(MainPanelContent):

    def __init__(self, main_panel, main_nav, dataservice):
        super().__init__("FirstPage", main_panel, main_nav, dataservice)

    def show(self):
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns='auto 1fr').classes('w-full gap-0'):
                ui.label('Step').classes('border py-2 px-2 ')
                ui.label('one').classes('border py-2 px-2 ')

main_content_items = [FirstPage]

class MainWindow2(MainWindow):

    def __init__(self, dataservice):
        super().__init__(dataservice, main_content_items)
        

            
