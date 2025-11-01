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

    def __init__(self, main_panel, main_nav):
        super().__init__("FirstPage", main_panel, main_nav)

    def show(self):
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns='auto 1fr').classes('w-full gap-0'):
                ui.label('Step').classes('border py-2 px-2 ')
                ui.label('one').classes('border py-2 px-2 ')

main_content_items = [FirstPage]

class UIApp:

    def __init__(self, data_dir, gnucash_path=None):
        self.main_flow = MainFlow(data_dir, gnucash_path)
        self.main_window = MainWindow(self)
        
class MainWindow2:

    def __init__(self, ui_app, initial_pages=None):
        self.ui_app = ui_app
        self.header = None
        self.left_drawer = None
        self.main_panel = None
        self.footer = None
        def toggle_left():
            self.left_drawer.toggle()
        with ui.header().classes(replace='row items-center') as self.header:
            ui.button(on_click=toggle_left, icon='menu').props('flat color=white')
        self.left_drawer = ui.left_drawer().classes('bg-blue-100') 
        self.main_panel = ui.element('div').classes('w-full')
        self.footer = ui.footer()
        self.main_nav = MainNav(self.left_drawer, self)
        if main_content_items is None:
            main_content_items = default_main_content_items
        for index, item in enumerate(main_content_items):
            page = item(self.main_panel, self.main_nav)
            if index == 0:
                first = page
            self.main_nav.add_main_panel_content(page)
        self.main_nav.show_main_content(first.name)
        

            
