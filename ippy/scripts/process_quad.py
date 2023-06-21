#!/usr/bin/env python3

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import MySQLdb

ippy_parent_dir = str(Path(__file__).resolve().parents[2])
if ippy_parent_dir not in sys.path:
    sys.path.append(ippy_parent_dir)

from ippy.constants import SCIDBM, SCIDBS1, SCIDBS2
from ippy.misc import infer_inst_from_expname

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process a quad to selected stage (e.g., WWDiff) with given expnames, label, and reduction class."
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
        datagroup and the workdir.""",
    )
    parser.add_argument(
        "--end_stage",
        default="wwdiff",
        choices=["chip", "camera", "fake", "warp", "wwdiff", "wsdiff"],
        help="The end stage of the processing. Default: wwdiff",
    )
    parser.add_argument(
        "--db_host",
        default="scidbs1",
        choices=["scidbm", "scidbs1", "scidbs2"],
        help="The MySQL database host for queries. Default: scidbs1",
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
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="Rerun the commands to complete the processing if previously interupted by e.g., Ctrl-C while waiting for warps to complete. Default: False when the flag is not specified.",
    )
    args = parser.parse_args()
    # pick the database host for queries
    SCIDB = eval(args.db_host.upper())
    # check if the four expnames are from the same quad
    if len(args.expnames) != 4:
        raise ValueError("Four expnames are required to form a quad.")
    insts = [infer_inst_from_expname(expname) for expname in args.expnames]
    inst = set(insts)
    if len(inst) != 1:
        raise ValueError("The four expnames must be from the same instrument.")
    inst = list(inst)[0]
    query = f"select exp_name, exp_id, dateobs, object, comment from rawExp where exp_name in {tuple(args.expnames)}"
    # print(query)
    db_conn = MySQLdb.connect(
        host=SCIDB.node,
        db=inst,
        user=SCIDB.user,
        passwd=SCIDB.password,
    )
    db_cursor = db_conn.cursor()
    db_cursor.execute(query)
    result = db_cursor.fetchall()
    db_cursor.close()
    db_conn.close()
    if len(result) == 4:
        exp_ids = [r[1] for r in result]
        dateobses = [r[2] for r in result]
        objects = [r[3] for r in result]
        comments = [r[4] for r in result]
    else:
        raise ValueError("Less/more than four matching expnames found in the database.")
    # check if the four expnames are from the same night
    if not all(dateobs.date() == dateobses[0].date() for dateobs in dateobses):
        raise ValueError("The four expnames must be from the same night.")
    chunk_object_names = [comment.rsplit(maxsplit=2)[0] for comment in comments]
    visit_nums = [int(comment.rsplit(maxsplit=1)[-1]) for comment in comments]
    if len(set(objects)) != 1 or len(set(chunk_object_names)) != 1:
        raise ValueError("The four expnames must have the same chunk and object name.")
    if set(visit_nums) != set(range(1, 5)):
        raise ValueError("The four expnames must have visit numbers from 1 to 4.")
    # queue the processing from chip to warp
    run_chiptool_cmd = [
        Path(__file__).resolve().parent / "run_chiptool.py",
        args.label,
        args.reduction,
        *args.expnames,
    ]
    if args.workdir is not None:
        run_chiptool_cmd.extend(["--workdir", args.workdir])
    else:
        args.workdir = f"neb://@HOST@.0/{args.label}/{args.reduction}"
    date = datetime.utcnow().strftime("%Y%m%d")
    data_group = f"{args.label}.{args.reduction}.{date}"
    if args.version is not None:
        data_group += f".v{args.version}"
        args.workdir += f".v{args.version}"
        run_chiptool_cmd.extend(["--version", args.version])
    if (
        args.end_stage == "chip"
        or args.end_stage == "camera"
        or args.end_stage == "fake"
    ):
        run_chiptool_cmd.extend(["--end_stage", args.end_stage])
    if args.commit:
        run_chiptool_cmd.append("--commit")
    if not args.rerun:
        subprocess.run(run_chiptool_cmd, check=True)
    else:
        # check if the chipRun already exists which means queueing from chip to warp was successful
        db_conn = MySQLdb.connect(
            host=SCIDB.node,
            db=inst,
            user=SCIDB.user,
            passwd=SCIDB.password,
        )
        db_cursor = db_conn.cursor()
        query = f"""
            select exp_name, exp_id, substring_index(comment,' ',-1) visit,
            chip_id, chipRun.state chip_state 
            from rawExp join chipRun using (exp_id)
            where exp_id in {tuple(exp_ids)}
            and (chipRun.label like "{args.label}" and chipRun.data_group like "{data_group}" and chipRun.reduction like "{args.reduction}")
        """
        db_cursor.execute(query)
        result = db_cursor.fetchall()
        if not result:
            subprocess.run(run_chiptool_cmd, check=True)
    if args.commit and (args.end_stage == "wwdiff" or args.end_stage == "wsdiff"):
        time_start = time.time()
        print("Waiting for the warp products to be ready...")
        try:
            # wait for 5 minutes to begin checking if the warp products are ready
            if not args.rerun:
                time.sleep(300)
            # checking if the warp products are ready
            db_conn = MySQLdb.connect(
                host=SCIDB.node,
                db=inst,
                user=SCIDB.user,
                passwd=SCIDB.password,
            )
            db_cursor = db_conn.cursor()
            query = f"""
                select exp_name, exp_id, substring_index(comment,' ',-1) visit,
                chip_id, chipRun.state chip_state, 
                cam_id, camRun.state cam_state, camProcessedExp.quality cam_quality, camProcessedExp.fwhm_major cam_fwhm_major,
                warp_id, warpRun.state warp_state 
                from rawExp 
                left join chipRun using (exp_id)
                left join camRun using (chip_id) 
                left join camProcessedExp using (cam_id)
                left join fakeRun using (cam_id) 
                left join warpRun using (fake_id) 
                where exp_id in {tuple(exp_ids)}
                and (chipRun.label is NULL or (chipRun.label like "{args.label}" and chipRun.data_group like "{data_group}" and chipRun.reduction like "{args.reduction}"))
                and (warpRun.state like "full" or (camRun.state like "full" and camProcessedExp.quality > 0))
                order by visit
            """
            while True:
                # query for completed processing from chip to warp, including those failed at cam stage
                db_cursor.execute(query)
                result = db_cursor.fetchall()
                if len(result) == 4:
                    db_cursor.close()
                    db_conn.close()
                    if args.rerun:
                        print(
                            f"All four exposures are processed. Now trying to queue wwdiff."
                        )
                    else:
                        print(
                            f"All four exposures are processed in {(time.time()-time_start)/60:.1f} minutes. Now trying to queue wwdiff."
                        )
                    good_warp_ids = [r[-2] for r in result if r[-1] == "full"]
                    if len(good_warp_ids) == 4:
                        warp_id_pairs = [
                            (good_warp_ids[0], good_warp_ids[1]),
                            (good_warp_ids[2], good_warp_ids[3]),
                        ]
                    elif len(good_warp_ids) == 3:
                        warp_id_pairs = [
                            (good_warp_ids[0], good_warp_ids[1]),
                            (good_warp_ids[1], good_warp_ids[2]),
                        ]
                    elif len(good_warp_ids) == 2:
                        warp_id_pairs = [(good_warp_ids[0], good_warp_ids[1])]
                    else:
                        warp_id_pairs = []
                    break
                else:
                    time.sleep(20)
            # queue wwdiff and/or wsdiff
            if args.end_stage == "wwdiff":
                for warp_id_pair in warp_id_pairs:
                    # difftool -dbname gpc1 -definewarpwarp -warp_id 2466230 -template_warp_id 2466231 -backwards -set_workdir neb://@HOST@.0/gpc1/HG.tests/HG.testops.20220726 -set_dist_group NULL -set_label HG.testops -set_data_group HG.testops.20220726 -set_reduction SWEETSPOT -simple -rerun -pretend
                    run_difftool_cmd = [
                        "difftool",
                        "-dbname",
                        inst,
                        "-definewarpwarp",
                        "-warp_id",
                        str(warp_id_pair[0]),
                        "-template_warp_id",
                        str(warp_id_pair[1]),
                        "-backwards",
                        "-set_workdir",
                        args.workdir,
                        "-set_dist_group",
                        "NULL",
                        "-set_label",
                        args.label,
                        "-set_data_group",
                        data_group,
                        "-set_reduction",
                        args.reduction,
                        "-simple",
                        "-rerun",
                    ]
                    if not args.commit:
                        run_difftool_cmd.append("-pretend")
                    print(" ".join(run_difftool_cmd))
                    subprocess.run(run_difftool_cmd, check=True)
        except KeyboardInterrupt:
            print(
                "Keyboard interruption. Please rerun the script with --rerun to complete the processing."
            )
            sys.exit(1)
