#!/usr/bin/env python3

import argparse
import subprocess
import sys
import time
from datetime import datetime
from itertools import chain
from pathlib import Path

import numpy as np
from astropy.table import Table

ippy_parent_dir = str(Path(__file__).resolve().parents[2])
if ippy_parent_dir not in sys.path:
    sys.path.append(ippy_parent_dir)

from ippy.misc import expname_pattern, infer_inst_from_expname

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run chiptool with given expnames, label, and reduction class."
    )
    parser.add_argument("label", help="The label for this run")
    parser.add_argument("reduction", help="The reduction class for this run")
    parser.add_argument(
        "expnames",
        type=str,
        nargs="+",
        help="Exposure names or files that contain the exposure names to be processed. Separated by space.",
    )
    parser.add_argument(
        "--version",
        help="""Version of the processing. Sometimes processing may fail or the data were processed with undesired setup. 
        In this case, we need to reprocess the data with a new version number. It will be used to append to the end of the 
        datagroup and default workdir.""",
    )
    parser.add_argument(
        "--end_stage",
        default="warp",
        choices=["chip", "camera", "fake", "warp"],
        help="The end stage of the processing. Default: warp",
    )
    parser.add_argument(
        "--workdir",
        help="workdir to store the processed products. Default: neb://@HOST@.0/label/reduction.date",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Commit to queue the processing. Default: False when the flag is not specified so chiptool will run with -pretend.",
    )
    args = parser.parse_args()
    date = datetime.utcnow().strftime("%Y%m%d")
    if args.workdir is None:
        args.workdir = f"neb://@HOST@.0/{args.label}/{args.reduction}.{date}"
    datagroup = f"{args.label}.{args.reduction}.{date}"
    if args.version is not None:
        args.workdir += f".v{args.version}"
        datagroup += f".v{args.version}"
    common_opts = [
        "-definebyquery",
        "-set_reduction",
        args.reduction,
        "-set_label",
        args.label,
        "-set_data_group",
        datagroup,
        "-set_end_stage",
        args.end_stage,
        "-set_tess_id",
        "RINGS.V3",
        "-set_workdir",
        args.workdir,
        "-simple",
    ]
    if not args.commit:
        common_opts.append("-pretend")
    cnt_valid_expnames = 0
    valid_expnames = []
    for expname in args.expnames:
        if expname_pattern.match(expname):
            valid_expnames.append(expname)
            cnt_valid_expnames += 1
    # maybe the user provided a file containing expnames
    if cnt_valid_expnames == 0:
        for expname_file in args.expnames:
            if Path(expname_file).is_file():
                t_expnames = Table.read(expname_file, format="ascii.no_header")
                # flatten the table if it has multiple columns
                t_expnames = list(chain.from_iterable(np.ravel(t_expnames).tolist()))
                for expname in t_expnames:
                    if expname_pattern.match(expname):
                        valid_expnames.append(expname)
                        cnt_valid_expnames += 1
        if cnt_valid_expnames == 0:
            raise ValueError("No valid exposure name found.")
    for expname in valid_expnames:
        dbname = infer_inst_from_expname(expname)
        print("chiptool", *common_opts, "-exp_name", expname, "-dbname", dbname)
        try:
            run_chiptool = subprocess.run(
                ["chiptool", *common_opts, "-exp_name", expname, "-dbname", dbname],
                text=True,
                capture_output=True,
                check=True,
            )
            print(run_chiptool.stdout)
        except subprocess.CalledProcessError as e:
            print(
                f"Command '{' '.join(e.cmd)}' returned non-zero exit status, please check its stderr below."
            )
            # print(e.stdout)
            print(e.stderr)
            sys.exit(1)
        time.sleep(0.01)
