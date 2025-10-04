from dataclasses import dataclass
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

    def __init__(self, layout, current_panel):
        self.layout = layout
        self.current_panel = current_panel
        
class Panel:

    def __init__(self):
        showing = False

    def show(self):
        self.fill_panel()
        showing = True
    
class MainWindow:

    def __init__(self, dataservice):
        self.dataservice = dataservice
        self.header = None
        self.left_drawer = None
        self.main_panel = None
        self.footer = None
        self.main_content = None
        
        with ui.header().classes(replace='row items-center') as self.header:
            ui.button(on_click=lambda: left_drawer.toggle(), icon='menu').props('flat color=white')
        self.left_drawer = ui.left_drawer().classes('bg-blue-100') 
        self.main_panel = ui.element('div').classes('w-full')
        self.footer = ui.footer()


    def show_page_by_name(self, name):
        p_map = {
            "Home": self.show_main_page,
            "Accounts": self.show_accounts_page,
            "Matchers": self.show_matchers_page,
            "Transactions": self.show_transactions_page,
            }
        target = p_map[name]
        target()

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
        self.main_content = "Home"
        self.update_nav()

    def show_accounts_page(self):
        self.main_panel.clear()
        with self.main_panel:
            ui.label('accounts')
        self.main_content = "Accounts"
        self.update_nav()

    def show_matchers_page(self):
        self.main_panel.clear()
        with self.main_panel:
            ui.label('matchers')
        self.main_content = "Matchers"
        self.update_nav()

    def show_transactions_page(self):
        self.main_panel.clear()
        with self.main_panel:
            ui.label('transactions')
        self.main_content = "Transactions"
        self.update_nav()

    def update_nav(self):
        self.left_drawer.clear()
        with self.left_drawer:
            with ui.list().props('bordered separator').classes('w-full'):
                ui.separator()
                for page_name in ['Home', 'Accounts', 'Matchers', 'Transactions']:
                    if page_name == self.main_content:
                        with ui.item():
                            with ui.item_section():
                                ui.item_label(page_name).classes('text-xl text-bold')
                    else:
                        with ui.item(on_click=lambda page_name=page_name: self.show_page_by_name(page_name)):
                            with ui.item_section():
                                ui.item_label(page_name)



