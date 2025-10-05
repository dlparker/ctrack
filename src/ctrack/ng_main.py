from dataclasses import dataclass
import re
from pathlib import Path
from typing import Optional, Any
from nicegui import ui
from nicegui.element import Element

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
        self.stnd_items = {
            "Home": self.main_window.show_main_page,
            "Accounts": self.main_window.show_accounts_page,
            "Matchers": self.main_window.show_matchers_page,
            "Transactions": self.main_window.show_transactions_page,
            }
        self.main_content  = "Home"
        self.dyn_items = {}

    def show_page_by_name(self, name):
        if name in self.stnd_items:
            target = self.stnd_items[name]
            target()
            self.main_content = name
            return
        if name in self.dyn_items:
            target = self.dyn_items[name]
            target()
            self.main_content = name
            return
    
    def add_dyn_item(self, name, operation):
        self.dyn_items[name] = operation

    def update_menu(self, name=None):
        if name is not None:
            self.main_content = name
        self.nav_container.clear()
        with self.nav_container:
            with ui.list().props('bordered separator').classes('w-full'):
                ui.separator()
                for page_name in self.stnd_items:
                    if page_name == self.main_content:
                        with ui.item():
                            with ui.item_section():
                                ui.item_label(page_name).classes('text-xl text-bold')
                    else:
                        with ui.item(on_click=lambda page_name=page_name: self.show_page_by_name(page_name)):
                            with ui.item_section():
                                ui.item_label(page_name)
                for x_name in self.dyn_items:
                    if x_name == self.main_content:
                        with ui.item():
                            with ui.item_section():
                                ui.item_label(x_name).classes('text-xl text-bold')
                    else:
                        with ui.item(on_click=lambda x_name=x_name: self.show_page_by_name(x_name)):
                            with ui.item_section():
                                ui.item_label(x_name)

class Panel:

    def __init__(self):
        showing = False

    def show(self):
        self.fill_panel()
        showing = True

class MatcherBuildElements:

    def __init__(self, description, regexp=None, no_case=True, classes='border px-2'):
        self.description = description
        self.regexp = regexp
        self.no_case = no_case
        self.matches = True
        self.desc_label =  ui.label(self.description).classes(classes)
        if self.regexp is None:
            self.regexp = f"^{re.escape(self.description)}"
        def check_match(new_regexp):
            if self.no_case:
                compiled = re.compile(new_regexp, re.IGNORECASE)
            else:
                compiled = re.compile(new_regexp)
            if compiled.match(self.description):
                self.regexp = new_regexp
                self.cb.value = True
                return True
            self.cb.value = False
            return False
        self.input = ui.input(value=self.regexp,
                              validation={"Does not match": lambda value:check_match(value)},
                              ).classes(classes)
        self.switch = ui.switch("Ignore Case", value=self.no_case).classes(classes)
        self.switch.bind_value(self, 'no_case')
        self.cb = ui.checkbox("", value=self.matches).classes(classes)
        check_match(self.regexp)

class MainWindow:

    def __init__(self, dataservice):
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

    def show_page_by_name(self, name):
        self.main_nav.show_page_name(name)

    def show_main_page(self):
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns=2):
                ui.label('Accounts known:')
                ui.label(self.dataservice.known_accounts_count())

                ui.label('Matchers:')
                ui.label(self.dataservice.matchers_count())

                ui.label('Transaction Sets:')
                set_count, total = self.dataservice.transaction_sets_stats()
                ui.label(set_count)

                ui.label('Total Transactions:')
                ui.label(total)
        self.main_nav.update_menu("Home")

    def show_accounts_page(self):
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns='auto 1fr').classes('w-full gap-0'):
                ui.label('Path').classes('border py-2 px-2 ')
                ui.label('Description').classes('border py-2 px-2')
                for account in self.dataservice.get_known_accounts():
                    ui.label(account.account_path).classes('border py-1 px-2')
                    ui.label(account.description).classes('border py-1 px-2')
        self.main_nav.update_menu("Accounts")

    def show_matchers_page(self):
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
        self.main_nav.update_menu("Matchers")

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
                b = ui.button(button_text,
                              on_click=lambda name=tset.load_path, index=index: self.make_edit_matcher(name, index))
                b.props('flat dense').classes('border border-black px-2 ')
                for col in row.raw:
                    ui.label(col).classes('border px-2 ')

        if len(unmatched) > 0:
            
            with ui.grid(columns="auto 5fr 1fr 1fr 1fr").classes('w-full gap-0'):
                ui.label("Description").classes('border py-2 px-2 ')
                ui.label("Regexp").classes('border py-2 px-2 ')
                ui.label("Ignorcase").classes('border py-2 px-2 ')
                ui.label("Match").classes('border py-2 px-2 ')
                ui.label("Save").classes('border py-2 px-2 ')
                for um in unmatched:
                    mbe = MatcherBuildElements(um.description, regexp=None,
                                               no_case=True, classes='border px-2')
                    def save_matcher(mbe):
                        print("gonna save")
                        pass
                    ui.button("Save", on_click=lambda mbe:save_matcher(mbe)).classes('border py-2 px-2 ')
                    
    def show_transactions_page(self):
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
        self.main_nav.update_menu("Transactions")

    def make_edit_matcher(self, tset_name, row_index):

        fname = Path(tset_name).parts[-1]
        nav_name = f"{fname}:\nrow-{row_index}"
        def show_page(tset_path, index):
            sets = self.dataservice.get_transaction_sets()
            tset = sets[tset_path]
            self.main_panel.clear()
            with self.main_panel:
                ui.label(tset.load_path)
                ui.label(tset.rows[index].description)
        self.main_nav.add_dyn_item(nav_name, lambda tset_name=tset_name, index=row_index:show_page(tset_name, index))
        self.main_nav.update_menu(nav_name)
        self.main_nav.show_page_by_name(nav_name)

    def show_edit_matcher(self, tset, row_index):
        pass
