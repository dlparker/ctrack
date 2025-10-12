from dataclasses import dataclass
import re
from pathlib import Path
from typing import Optional, Any
from nicegui import ui
from nicegui.element import Element
from ctrack.data_service import MatcherRule, Account

@dataclass
class MainLayout:
    header: Element
    left_drawer: Element
    main_panel: Element
    footer: Element

class MainNav:

    def __init__(self, nav_container, main_window):
        self.nav_container = nav_container
        self.main_window = main_window
        self.main_content  = "Home"
        self.dyn_items = {}
        self.main_content_objs = {}

    def show_page_by_name(self, name):
        if name in self.dyn_items:
            target = self.dyn_items[name]
            target()
            self.main_content = name
            return

    def add_main_panel_content(self, content):
        self.main_content_objs[content.name] = content

    def show_main_content(self, name):
        self.main_content_objs[name].show()
        self.main_content = name
        self.update_menu()

    def remove_main_panel_content(self, name, route_to=None):
        if route_to:
            self.show_main_content(route_to)
        del self.main_content_objs[name]
        self.update_menu()

    def update_menu(self, name=None):
        if name is not None:
            self.main_content = name
        self.nav_container.clear()
        with self.nav_container:
            with ui.list().props('bordered separator').classes('w-full'):
                ui.separator()
                for content_name in self.main_content_objs:
                    if content_name == self.main_content:
                        with ui.item():
                            with ui.item_section():
                                ui.item_label(content_name).classes('text-xl text-bold')
                    else:
                        with ui.item(on_click=lambda content_name=content_name: self.show_main_content(content_name)):
                            with ui.item_section():
                                ui.item_label(content_name)

class MainPanelContent:

    def __init__(self, name, main_panel, main_nav, dataservice):
        self.name = name
        self.main_panel = main_panel
        self.main_nav = main_nav
        self.dataservice = dataservice
        self.main_nav.add_main_panel_content(self)
        
    def show(self):
        self.main_panel.clear()
        with self.main_panel:
            ui.label('placeholder page')

class AccountsPage(MainPanelContent):

    def __init__(self, main_panel, main_nav, dataservice):
        super().__init__("Accounts", main_panel, main_nav, dataservice)

    def show(self):
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns='auto 1fr').classes('w-full gap-0'):
                ui.label('Path').classes('border py-2 px-2 ')
                ui.label('Description').classes('border py-2 px-2')
                for account in self.dataservice.get_accounts():
                    ui.label(account.account_path).classes('border py-1 px-2')
                    ui.label(account.description).classes('border py-1 px-2')


class MatchersPage(MainPanelContent):

    def __init__(self, main_panel, main_nav, dataservice):
        super().__init__("Matchers", main_panel, main_nav, dataservice)

    def show(self):
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns='auto auto 1fr').classes('w-full gap-0'):
                ui.label('Regexp').classes('border py-2 px-2 ')
                ui.label('NoCase').classes('border py-2 px-2 ')
                ui.label('Account Path').classes('border py-2 px-2')
                for matcher in self.dataservice.get_matchers():
                    ui.label(matcher.regexp).classes('border py-1 px-2')
                    ui.label(str(matcher.no_case)).classes('border py-1 px-2')
                    ui.label(matcher.account_path).classes('border py-1 px-2')
                    
class SummaryPage(MainPanelContent):

    def __init__(self, main_panel, main_nav, dataservice):
        super().__init__("Summary", main_panel, main_nav, dataservice)

    def show(self):
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns=2):
                ui.label('Accounts known:')
                ui.label(self.dataservice.accounts_count())

                ui.label('Matchers:')
                ui.label(self.dataservice.matchers_count())

                ui.label('Transaction Sets:')
                set_count, total = self.dataservice.transaction_sets_stats()
                ui.label(set_count)

                ui.label('Total Transactions:')
                ui.label(total)

class TransactionFilesPage(MainPanelContent):

    def __init__(self, main_panel, main_nav, dataservice):
        super().__init__("Transactions", main_panel, main_nav, dataservice)

    def show(self):
        self.main_panel.clear()
        with self.main_panel:
            tab_items = {}
            tab_set_names = {}
            sets =  self.dataservice.get_transaction_sets()
            with ui.tabs().classes('w-full') as tabs:
                files_tab = ui.tab('Files')
                tab_items["files"] = files_tab
                for tset_name in sets:
                    t_name = Path(tset_name).parts[-1]
                    tab_items[t_name] = ui.tab(t_name)
                    tab_set_names[t_name] = tset_name
            with ui.tab_panels(tabs, value="Files").classes('w-full') as tabs:
                with ui.tab_panel(files_tab):
                    with ui.grid(columns="auto auto auto auto").classes('w-full gap-0'):
                        ui.label("File").classes('border py-2 px-2 ')
                        ui.label("Mapped").classes('border py-2 px-2 ')
                        ui.label("Matched").classes('border py-2 px-2 ')
                        ui.label("Unmatched").classes('border py-2 px-2 ')
                        for path, tset in sets.items():
                            ui.label(path).classes('border py-2 px-2 ')
                            if tset.column_map:
                                ui.label("True").classes('border py-2 px-2 ')
                                matched = unmatched = 0
                                for row in tset.rows:
                                    if row.matcher:
                                        matched += 1
                                    else:
                                        unmatched += 1
                                ui.label(matched).classes('border py-2 px-2 ')
                                ui.label(unmatched).classes('border py-2 px-2 ')
                            else:
                                ui.label("False").classes('border py-2 px-2 ')
                                ui.label("").classes('border py-2 px-2 ')
                                ui.label("").classes('border py-2 px-2 ')
                for tab_name in tab_items:
                    if tab_name == "files":
                        continue
                    tab = tab_items[tab_name]
                    set_name = tab_set_names[tab_name]
                    t_set = sets[set_name] 
                    with ui.tab_panel(tab):
                        self.show_transaction_file_path(tab, tset_name, tset)
                
    def show_transaction_file_path(self, tab, tset_name, tset):
        ui.label(f'File path = {tset_name}')
        if tset.column_map is None:
            ui.label("No column map matches this file").classes('text-lg text-bold')
        tmp = ["auto"] # for "matched" column
        for fname in tset.column_names:
            tmp.append("auto")
        cstring = " ".join(tmp)
        unmatched = []
        with ui.grid(columns=cstring).classes('w-full gap-0'):
            ui.label("Matched").classes('border py-2 px-2 ')
            for cname in tset.column_names:
                ui.label(cname).classes('border py-2 px-2 ')
            if tset.column_map:
                ui.label("").classes('border py-2 px-2 ')
                for cname in tset.column_names:
                    if cname == tset.column_map.date_col_name:
                        ui.label("* DATE *").classes('border py-2 px-2 ')
                    elif cname == tset.column_map.desc_col_name:
                        ui.label("* DESCRIPTION *").classes('border py-2 px-2 ')
                    elif cname == tset.column_map.amt_col_name:
                        ui.label("* AMOUNT *").classes('border py-2 px-2 ')
                    else:
                        ui.label("").classes('border py-2 px-2 ')
            for index, row in enumerate(tset.rows):
                if row.matcher:
                    button_text = "Edit"
                else:
                    button_text = "Add"
                    unmatched.append(row)
                # Create a button styled to look like a label
                def setup_matcher(name, index):
                    page = EditMatcherPage(name, index, self.main_panel, self.main_nav, self.dataservice)
                    self.main_nav.add_main_panel_content(page)
                    self.main_nav.show_main_content(page.name)
                b = ui.button(button_text,
                              on_click=lambda name=tset.load_path, index=index: setup_matcher(name, index))
                b.props('flat dense').classes('border border-black px-2 ')
                for col in row.raw:
                    ui.label(col).classes('border px-2 ')
                    
class MatchForEdit:
    
    def __init__(self, tset, row_index):
        self.tset = tset
        self.row_index = row_index
        self.row = tset.rows[row_index]

        self.regexp = None
        self.no_case = True
        self.switch = None
        self.matches = True
        self.account_path = None
        if self.row.matcher is None:
            self.regexp = f"^{re.escape(self.row.description)}"
            self.no_case = True
            self.account_path = "Expenses:"
        else:
            self.regexp = self.row.matcher.regexp
            self.no_case = self.row.matcher.no_case
            self.account_path = self.row.matcher.account_path
        self.matches = True

    def check_match(self, new_regexp=None):
        if new_regexp is None:
            new_regexp = self.regexp
        if self.no_case:
            compiled = re.compile(new_regexp, re.IGNORECASE)
        else:
            compiled = re.compile(new_regexp)
        if compiled.match(self.row.description):
            self.regexp = new_regexp
            self.matches = True
            return True
        self.matches = False
        return False
    
class EditMatcherPage(MainPanelContent):

    def __init__(self, transaction_set_load_path, row_index, main_panel, main_nav, dataservice):
        self.path = Path(transaction_set_load_path)
        self.row_index = row_index
        fname = self.path.parts[-1]
        self.nav_name = f"{fname}:\nrow-{row_index}"
        super().__init__(self.nav_name, main_panel, main_nav, dataservice)
        self.tset = self.dataservice.get_transaction_set(self.path)
        self.row = self.tset.rows[row_index]

    def show(self):
        self.main_panel.clear()
        with self.main_panel:
            
            self.medit = MatchForEdit(self.tset, self.row_index)
            left_classes = "py-2 px-2"
            right_classes = "py-2 px-2"
            with ui.grid(columns="auto 1fr").classes('w-full gap-0'):
                ui.label("Description").classes(left_classes)
                ui.label(self.row.description).classes(right_classes)

                ui.label("Regexp").classes(left_classes)

                regexp_input = ui.input(value=self.medit.regexp,
                                        validation={"Does not match": lambda value:self.medit.check_match(value)},
                                        ).classes(right_classes)

                ui.label("Ignorcase").classes(left_classes)
                switch = ui.switch("", value=self.medit.no_case,
                                   on_change=self.medit.check_match,
                                   ).classes(right_classes)
                switch.bind_value(self.medit, 'no_case')

                ui.label("Match").classes(left_classes)
                check_box = ui.checkbox("", value=self.medit.matches).classes(right_classes)
                check_box.bind_value(self.medit, 'matches')
                check_box.disable()

                ui.label("Account").classes(left_classes)
                options = [account.account_path for account in self.dataservice.get_accounts()]
                regexp_input = ui.input(value=self.medit.account_path, autocomplete=options).classes(right_classes)
                regexp_input.bind_value(self.medit, 'account_path')

            with ui.grid(columns="1fr auto auto").classes('w-full gap-0'):
                ui.label()
                ui.button("Cancel", on_click=self.cleanup).classes(right_classes)
                save_button = ui.button("Save", on_click=self.save_and_cleanup).classes(right_classes)
                save_button.bind_enabled(self.medit, 'matches')
            
    def save_and_cleanup(self):
        if self.row.matcher is None:
            matcher = MatcherRule(regexp=self.medit.regexp,
                                  no_case=self.medit.no_case,
                                  account_path=self.medit.account_path)
            self.row.matcher = matcher
        self.dataservice.save_matcher_rule(self.row.matcher)
        options = [account.account_path for account in self.dataservice.get_accounts()]
        if self.medit.account_path not in options:
            page = EditAccountPage(self.medit.account_path, self.main_panel, self.main_nav, self.dataservice)
            self.main_nav.add_main_panel_content(page)
            self.main_nav.show_main_content(page.name)
            self.main_nav.remove_main_panel_content(self.nav_name)
        else:
            self.main_nav.remove_main_panel_content(self.nav_name, "Transactions")

    def cleanup(self):
        self.main_nav.remove_main_panel_content(self.nav_name, "Transactions")
                
                
class EditAccountPage(MainPanelContent):

    def __init__(self, account_path, main_panel, main_nav, dataservice):
        self.account_path = account_path
        self.nav_name = account_path.split(':')[-1]
        super().__init__(self.nav_name, main_panel, main_nav, dataservice)
        self.account = None

    def show(self):
        self.main_panel.clear()
        with self.main_panel:
            self.account = self.dataservice.get_account(self.account_path)
            if not self.account:
                self.account = Account(account_path=self.account_path, description="")
            left_classes = "py-2 px-2"
            right_classes = "py-2 px-2"
            self.main_panel.clear()
            with self.main_panel:
                with ui.grid(columns="auto 1fr").classes('w-full gap-0'):
                    ui.label("Name").classes(left_classes)
                    ui.label(self.account.account_path).classes(right_classes)
                    ui.label("Description").classes(left_classes)
                    desc_input = ui.input(value=self.account.description).classes(right_classes)
                    desc_input.bind_value(self.account, 'description')
                with ui.grid(columns="1fr auto auto").classes('w-full gap-0'):
                    ui.label()
                    ui.button("Cancel",on_click=self.cleanup).classes(right_classes)
                    ui.button("Save",on_click=self.save_and_cleanup).classes(right_classes)
        
    def cleanup(self):
        self.main_nav.remove_main_panel_content(self.nav_name, "Accounts")

    def save_and_cleanup(self):
        self.dataservice.save_account(self.account)
        self.cleanup()

default_main_content_items = [SummaryPage, AccountsPage, MatchersPage, TransactionFilesPage]
                    
class MainWindow:

    def __init__(self, dataservice, main_content_items=None):
        self.dataservice = dataservice
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
            page = item(self.main_panel, self.main_nav, self.dataservice)
            if index == 0:
                first = page
            self.main_nav.add_main_panel_content(page)
        self.main_nav.show_main_content(first.name)

            
