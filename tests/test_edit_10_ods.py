#!/usr/bin/env python
import shutil
from pathlib import Path
from ctrack.check_phases import check_10
from ctrack.edit_phases import edit_10_gen_ods, edit_10_reload_ods, edit_10_run_calc


def test_edit_10_ods():
    data_dir = Path(__file__).parent / "test_edit_10"
    misses = check_10(data_dir)
    edit_10_gen_ods(data_dir)
    #edit_10_run_calc(data_dir)
    edited_path = Path(data_dir) / "edit_10_edited.ods"
    op_path = Path(data_dir) / "edit_10.ods"
    shutil.copy(edited_path, op_path)
    edit_10_reload_ods(data_dir)

    
        
