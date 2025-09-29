#!/usr/bin/env python
import shutil
from pathlib import Path
from ctrack.check_phases import check_10
from ctrack.edit_phases import edit_10_gen_ods, edit_10_reload_ods, edit_10_run_calc


def test_edit_10_ods():
    pull_dir = Path(__file__).parent / "data_test_edit_10"
    data_dir = Path(__file__).parent / "target"
    for item in data_dir.glob("*"):
        item.unlink()
    for item in pull_dir.glob("*"):
        shutil.copy(item, data_dir)
    orig_map_path = data_dir / "matcher_map_orig.csv"
    map_path = data_dir / "matcher_map.csv"
    shutil.copy(orig_map_path, map_path)
    misses = check_10(data_dir)
    edit_10_gen_ods(data_dir)
    # in command line tool do this:
    # edit_10_run_calc(data_dir)

    # simulate human fixing things in ods file
    edited_path = Path(data_dir) / "edit_10_edited.ods"
    op_path = Path(data_dir) / "edit_10.ods"
    shutil.copy(edited_path, op_path)

    # now make sure that it did fix things
    edit_10_reload_ods(data_dir)
    finished_misses = check_10(data_dir)
    assert len(finished_misses) == 0
    

    
        
