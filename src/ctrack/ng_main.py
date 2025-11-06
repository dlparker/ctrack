from dataclasses import dataclass
import re
from pathlib import Path
from typing import Optional, Any
from nicegui import events, ui
from nicegui.element import Element
from ctrack.flow import MainFlow
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

    async def show_page_by_name(self, name):
        if name in self.dyn_items:
            target = self.dyn_items[name]
            await target()
            self.main_content = name
            return

    def add_main_panel_content(self, content):
        self.main_content_objs[content.name] = content

    async def show_main_content(self, name):
        await self.main_content_objs[name].show()
        self.main_content = name
        await self.update_menu()

    async def remove_main_panel_content(self, name, route_to=None):
        if route_to:
            await self.show_main_content(route_to)
        del self.main_content_objs[name]
        await self.update_menu()

    async def update_menu(self, name=None):
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

    def __init__(self, name, main_window):
        self.main_window = main_window
        self.name = name
        self.main_panel = main_window.main_panel
        self.main_nav = main_window.main_nav
        self.main_nav.add_main_panel_content(self)
        
    async def show(self):
        self.main_panel.clear()
        with self.main_panel:
            ui.label('placeholder page')

class GnuCashPage(MainPanelContent):

    page_name = "GnuCash File"

    def __init__(self, main_window):
        super().__init__(self.page_name, main_window)
        self.dataservice = self.main_window.ui_app.dataservice

    async def select_file_content(self):
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns='auto auto 4fr'):
                gcpicker = GnuCashPicker(self.main_window.ui_app.dataservice, self.show)
                ui.label('Gnucash').classes('py-2 px-2 ')
                ui.button('Choose File', on_click=gcpicker.pick_file, icon='folder')
        
    async def show(self):
        if self.dataservice.gnucash_path is None:
            await self.select_file_content()
            return
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns='auto auto '):
                ui.label('Gnucash File').classes('py-2 px-2 ')
                ui.label(self.dataservice.gnucash_path).classes('py-2 px-2 ')
        
class StatusPage(MainPanelContent):

    page_name = "Status"
    def __init__(self, main_window):
        super().__init__(self.page_name, main_window)
        self.dataservice = self.main_window.ui_app.dataservice
        
    async def show(self):
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns='auto auto 4fr'):
                ui.label('Gnucash File').classes('py-2 px-2 ')
                ui.button('Details',
                          on_click=lambda:self.main_nav.show_main_content(GnuCashPage.page_name))
                ui.label(self.dataservice.gnucash_path).classes('py-2 px-2 ')



class TFilesPage(MainPanelContent):

    page_name = "Transaction Files"
    
    def __init__(self, main_window):
        super().__init__(self.page_name, main_window)
        self.dataservice = self.main_window.ui_app.dataservice
        self.file_pages = {}
        
    async def show(self):
        self.main_panel.clear()
        with self.main_panel:
            tfpicker = TransactionFilePicker(self.main_window.ui_app.dataservice, self.show)
            ui.button("Add file", on_click=tfpicker.pick_file,
                      icon='folder').classes('py-2 px-2 ')

            with ui.grid(columns='auto auto 4fr'):
                ui.label('Status').classes('py-2 px-2 ')
                ui.label('Action').classes('py-2 px-2 ')
                ui.label('Path').classes('py-2 px-2 ')
                for workfile in self.dataservice.get_transaction_files(unsaved_only=True):
                    ui.label('Not saved').classes('py-2 px-2 ')
                    ui.button('Edit',
                              on_click=lambda workfile=workfile:self.edit_file(workfile)
                              ).classes('py-2 px-2 ')
                    ui.label(workfile.import_source_file).classes('py-2 px-2 ')
                for savedfile in self.dataservice.get_transaction_files(saved_only=True):
                    ui.label('Saved').classes('py-2 px-2 ')
                    ui.label('View').classes('py-2 px-2 ')
                    ui.label(savedfile.import_source_file).classes('py-2 px-2 ')

    async def edit_file(self, workfile):
        print(f"would edit {workfile}")
        if workfile.display_name not in self.file_pages:
            tfp = TFilePage(self.main_window, workfile)
            self.main_nav.add_main_panel_content(tfp)
        await self.main_nav.show_main_content(workfile.display_name)
              
        
class MatchersPage(MainPanelContent):

    page_name = "Matchers"
    
    def __init__(self, main_window):
        super().__init__(self.page_name, main_window)
        self.dataservice = self.main_window.ui_app.dataservice
        
    async def show(self):
        self.main_panel.clear()
        with self.main_panel:
            mfpicker = MatcherFilePicker(self.main_window.ui_app.dataservice, self.show)
            ui.button("Add from file", on_click=mfpicker.pick_file,
                      icon='folder').classes('py-2 px-2 ')

            with ui.grid(columns='auto auto 1fr').classes('w-full gap-0'):
                ui.label('Regexp').classes('border py-2 px-2 ')
                ui.label('NoCase').classes('border py-2 px-2 ')
                ui.label('Account Path').classes('border py-2 px-2')
                for matcher in self.dataservice.get_matchers():
                    ui.label(matcher.regexp).classes('border py-1 px-2')
                    ui.label(str(matcher.no_case)).classes('border py-1 px-2')
                    ui.label(matcher.account_name).classes('border py-1 px-2')


class TFilePage(MainPanelContent):

    
    def __init__(self, main_window, tfile_rec):
        self.tfile_rec = tfile_rec
        page_name = tfile_rec.display_name
        super().__init__(page_name, main_window)
        self.dataservice = self.main_window.ui_app.dataservice
        
    async def show(self):
        self.main_panel.clear()
        with self.main_panel:
            with ui.grid(columns='auto auto 1fr').classes('w-full gap-0'):
                ui.label('Regexp').classes('border py-2 px-2 ')
                ui.label('NoCase').classes('border py-2 px-2 ')
                ui.label('Account Path').classes('border py-2 px-2')

                ui.label('')
                ui.label(self.tfile_rec.display_name)
                ui.label(self.tfile_rec.import_source_file)
                    
    async def show(self):
        self.main_panel.clear()
        with self.main_panel:
            ui.label(f'File path = {self.tfile_rec.import_source_file}')
            if self.tfile_rec.col_map_id is None:
                ui.label("No column map matches this file").classes('text-lg text-bold')
                column_map = None
            else:
                column_map = self.dataservice.get_column_map(self.tfile_rec.col_map_id)
            tmp = ["auto"] # for "matched" column
            for fname in tset.column_names:
                tmp.append("auto")
            cstring = " ".join(tmp)
            unmatched = []
            with ui.grid(columns=cstring).classes('w-full gap-0'):
                ui.label("Matched").classes('border py-2 px-2 ')
                for cname in tset.column_names:
                    ui.label(cname).classes('border py-2 px-2 ')
                if column_map:
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


default_main_content_items = [StatusPage, GnuCashPage, TFilesPage, MatchersPage]
                    
class UIApp:

    def __init__(self, data_dir, gnucash_path=None):
        self.main_flow = MainFlow(data_dir, gnucash_path)
        self.dataservice = self.main_flow.dataservice
        self.main_window = MainWindow(self)

    async def start(self):
        await self.main_window.start()
        
class MainWindow:

    def __init__(self, ui_app, main_content_items=None):
        self.ui_app = ui_app
        self.main_flow = ui_app.main_flow
        self.header = None
        self.left_drawer = None
        self.main_panel = None
        self.footer = None
        if main_content_items is None:
            main_content_items = default_main_content_items
        self.main_content_items = main_content_items

    async def start(self):
        def toggle_left():
            self.left_drawer.toggle()
        with ui.header().classes(replace='row items-center') as self.header:
            ui.button(on_click=toggle_left, icon='menu').props('flat color=white')
        self.left_drawer = ui.left_drawer().classes('bg-blue-100') 
        self.main_panel = ui.element('div').classes('w-full')
        self.footer = ui.footer()
        self.main_nav = MainNav(self.left_drawer, self)
        for index, item in enumerate(self.main_content_items):
            page = item(self)
            if index == 0:
                first = page
            self.main_nav.add_main_panel_content(page)
        await self.main_nav.show_main_content(first.name)

            
class local_file_picker(ui.dialog):

    def __init__(self, directory: str, *,
                 upper_limit: Optional[str] = ..., multiple: bool = False,
                 files_only: bool = True,
                 show_hidden_files: bool = False) -> None:
        """Local File Picker

        This is a simple file picker that allows you to select a file from the local filesystem where NiceGUI is running.

        :param directory: The directory to start in.
        :param upper_limit: The directory to stop at (None: no limit, default: same as the starting directory).
        :param multiple: Whether to allow multiple files to be selected.
        :param files_only: allow only file selection, or also allow directory selection
        :param show_hidden_files: Whether to show hidden files.
        """
        super().__init__()
        self.path = Path(directory).resolve()
        if upper_limit is None:
            self.upper_limit = None
        else:
            self.upper_limit = Path(directory if upper_limit == ... else upper_limit).expanduser()
        self.show_hidden_files = show_hidden_files

        """
        """
        with self, ui.card():
            self.grid = ui.aggrid({
                'columnDefs': [{'field': 'name', 'headerName': 'File'}],
                'rowSelection': {
                    'mode': 'multiRow' if multiple else 'singleRow',
                    ':isRowSelectable': "params => !params.data.name.startsWith('📁')",
                    'hideDisabledCheckboxes': True,
                }
            }, html_columns=[0]).classes('w-96').on('cellDoubleClicked', self.handle_double_click)
            with ui.row().classes('w-full justify-end'):
                ui.button('Cancel', on_click=self.close).props('outline')
                ui.button('Ok', on_click=self._handle_ok)
        self.update_grid()

    def update_grid(self) -> None:
        paths = list(self.path.glob('*'))
        if not self.show_hidden_files:
            paths = [p for p in paths if not p.name.startswith('.')]
        paths.sort(key=lambda p: p.name.lower())
        paths.sort(key=lambda p: not p.is_dir())

        self.grid.options['rowData'] = [
            {
                'name': f'📁 <strong>{p.name}</strong>' if p.is_dir() else p.name,
                'path': str(p),
            }
            for p in paths
        ]
        if (self.upper_limit is None and self.path != self.path.parent) or \
                (self.upper_limit is not None and self.path != self.upper_limit):
            self.grid.options['rowData'].insert(0, {
                'name': '📁 <strong>..</strong>',
                'path': str(self.path.parent),
            })
        self.grid.update()

    def handle_double_click(self, e: events.GenericEventArguments) -> None:
        self.path = Path(e.args['data']['path'])
        if self.path.is_dir():
            self.update_grid()
        else:
            self.submit([str(self.path)])

    async def _handle_ok(self):
        rows = await self.grid.get_selected_rows()
        self.submit([r['path'] for r in rows])

class GnuCashPicker:

    def __init__(self, dataservice, done_callback=None):
        self.dataservice = dataservice
        self.done_callback = done_callback
        if self.dataservice.gnucash_path is not None:
            raise Exception('only one gnucash file allowed for one database')
        self.spath = "."
        self.ulimit = Path("~").expanduser()

    async def pick_file(self) -> None:
        result = await local_file_picker(self.spath, upper_limit = self.ulimit, multiple=False)
        if result:
            self.dataservice.load_gnucash_file(result[0])
            if self.done_callback:
                await self.done_callback()
            
class TransactionFilePicker:

    def __init__(self, dataservice, done_callback=None):
        self.dataservice = dataservice
        self.done_callback = done_callback
        self.spath = "."
        self.ulimit = Path("~").expanduser()

    async def pick_file(self) -> None:
        result = await local_file_picker(self.spath, upper_limit = self.ulimit,
                                         multiple=False)
        if result:
            self.dataservice.add_unmapped_transaction_file(result[0])
            if self.done_callback:
                await self.done_callback()
            

class MatcherFilePicker:

    def __init__(self, dataservice, done_callback=None):
        self.dataservice = dataservice
        self.done_callback = done_callback
        self.spath = "."
        self.ulimit = Path("~").expanduser()

    async def pick_file(self) -> None:
        result = await local_file_picker(self.spath, upper_limit = self.ulimit,
                                         multiple=False)
        if result:
            self.dataservice.load_matcher_file(result[0])
            if self.done_callback:
                await self.done_callback()
            


