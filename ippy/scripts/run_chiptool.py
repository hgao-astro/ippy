#!/usr/bin/env python3

import argparse
import subprocess
import sys
import time
from datetime import datetime
from itertools import chain
from pathlib import Path

import MySQLdb
import numpy as np
from astropy.table import Table

ippy_parent_dir = str(Path(__file__).resolve().parents[2])
if ippy_parent_dir not in sys.path:
    sys.path.append(ippy_parent_dir)

from ippy.constants import SCIDBS1
from ippy.misc import expname_pattern, infer_inst_from_expname

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run chiptool with given expnames, label, and reduction class."
    )
    parser.add_argument("label", help="The label for this run")
    parser.add_argument("reduction", help="The reduction class for this run")
    parser.add_argument(
        "--expnames",
        type=str,
        nargs="*",
        # default=None,
        help="Exposure names or files that contain the exposure names to be processed. Separated by space.",
    )
    parser.add_argument(
        "--chunk",
        type=str,
        nargs="*",
        # default=None,
        help="Chunk of exposures to be processed. Dateobs must be supplied as well. Separated by space.",
    )
    parser.add_argument(
        "--dateobs",
        type=str,
        nargs="*",
        # default=None,
        help="Dateobs of chunks of exposures to be processed. Must be in the same order. Separated by space.",
    )
    parser.add_argument(
        "--dbname",
        type=str,
        nargs="*",
        # default=None,
        help="dbname of chunks of exposures to be processed. Must be in the same order. Separated by space.",
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
        help="workdir to store the processed products. Default: neb://@HOST@.0/dbname/label/reduction.date",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Commit to queue the processing. Default: False when the flag is not specified so chiptool will run with -pretend.",
    )
    args = parser.parse_args()
    label = args.label
    reduction = args.reduction
    date = datetime.utcnow().strftime("%Y%m%d")
    datagroup = f"{label}.{reduction}.{date}"
    if args.version is not None:
        # workdir += f".v{args.version}"
        datagroup += f".v{args.version}"
    common_opts = [
        "-definebyquery",
        "-set_reduction",
        reduction,
        "-set_label",
        label,
        "-set_data_group",
        datagroup,
        "-set_end_stage",
        args.end_stage,
        "-set_tess_id",
        "RINGS.V3",
        # "-set_workdir",
        # args.workdir,
        "-simple",
    ]
    if not args.commit:
        common_opts.append("-pretend")
    if args.expnames is not None:
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
                    t_expnames = Table.read(expname_file, format="ascii")
                    # flatten the table if it has multiple columns
                    t_expnames = list(
                        chain.from_iterable(np.ravel(t_expnames).tolist())
                    )
                    for expname in t_expnames:
                        if expname_pattern.match(expname):
                            valid_expnames.append(expname)
                            cnt_valid_expnames += 1
            if cnt_valid_expnames == 0:
                raise ValueError("No valid exposure name found.")
    else:
        valid_expnames = []
        for chunk, dateobs, dbname in zip(args.chunk, args.dateobs, args.dbname):
            if chunk is None or dateobs is None or dbname is None:
                raise ValueError(
                    "chunk, dateobs, and dbname must be supplied together."
                )
            db_conn = MySQLdb.connect(
                host=SCIDBS1.node,
                user=SCIDBS1.user,
                passwd=SCIDBS1.password,
                db=dbname,
            )
            db_cursor = db_conn.cursor()
            query = f"""select exp_name, dateobs, reduction from rawExp  where (obs_mode like '%SS%' or obs_mode like '%BRIGHT%') 
            and obs_mode not like 'ENGINEERING' and obs_mode not like 'MANUAL' and exp_type like 'OBJECT' and comment like '%visit%' and 
            comment like '{chunk}%' and dateobs like '{dateobs}%'"""
            db_cursor.execute(query)
            result = db_cursor.fetchall()
            db_cursor.close()
            db_conn.close()
            valid_expnames.extend([r[0] for r in result])

    for expname in valid_expnames:
        dbname = infer_inst_from_expname(expname)
        if args.workdir is None:
            workdir = f"neb://@HOST@.0/{dbname}/{label}/{reduction}.{date}"
        else:
            workdir = eval(f"f'neb://@HOST@.0/{args.workdir}'")
        if args.version is not None:
            workdir += f".v{args.version}"
        run_chiptool_cmd = [
            "chiptool",
            *common_opts,
            "-exp_name",
            expname,
            "-dbname",
            dbname,
            "-set_workdir",
            workdir,
        ]
        print(" ".join(run_chiptool_cmd))
        try:
            run_chiptool = subprocess.run(
                run_chiptool_cmd,
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
