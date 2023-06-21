import argparse

# from typing import List, Tuple
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep

import MySQLdb

ippy_parent_dir = str(Path(__file__).resolve().parents[2])
if ippy_parent_dir not in sys.path:
    sys.path.append(ippy_parent_dir)

if sys.version_info[:2] >= (3, 7):
    from ippy.constants import SCIDB

    SCIDBS1_host = SCIDB.node
    SCIDBS1_USER = SCIDB.user
    SCIDBS1_PSW = SCIDB.password
else:
    from ippy.constants import MYSQL_PSW_READ_ONLY, MYSQL_USER_READ_ONLY, SCIDB

    SCIDBS1_host = SCIDB
    SCIDBS1_USER = MYSQL_USER_READ_ONLY
    SCIDBS1_PSW = MYSQL_PSW_READ_ONLY


def make_wwdiff(chunk_name, label, dateobs, dbname, pretend):
    # query for exposures and their processing status from chip to warp stage
    query = f"""
        select exp_name, exp_id, dateobs, object, substring_index(comment,' ',-1) visit,
        chip_id, chipRun.state chip_state, 
        cam_id, camRun.state cam_state, camProcessedExp.quality cam_quality, camProcessedExp.fwhm_major cam_fwhm_major,
        warp_id, warpRun.state warp_state 
        from rawExp 
        left join chipRun using (exp_id)
        left join camRun using (chip_id) 
        left join camProcessedExp using (cam_id)
        left join fakeRun using (cam_id) 
        left join warpRun using (fake_id) 
        where dateobs like '{dateobs}%'
        and comment like '{chunk_name}%'
        and chipRun.label like '{label}'
        order by exp_id
        """
    # print(query)
    db_conn = MySQLdb.connect(
        host=SCIDBS1_host,
        db=dbname,
        user=SCIDBS1_USER,
        passwd=SCIDBS1_PSW,
    )
    db_cursor = db_conn.cursor()
    db_cursor.execute(query)
    result = db_cursor.fetchall()
    if result:
        quad_names = set(r[3] for r in result)
        for quad in quad_names:
            # print("for quad", quad, "in", chunk_name, "on", dateobs, dbname)
            fwhms = [r[10] for r in result if r[3] == quad]
            if dbname == "gpc1":
                good_idx = [
                    i
                    for i in range(len(fwhms))
                    if fwhms[i] is not None and fwhms[i] < 12
                ]
            elif dbname == "gpc2":
                good_idx = [
                    i
                    for i in range(len(fwhms))
                    if fwhms[i] is not None and fwhms[i] < 100
                ]
            visit_nums = [int(r[4]) for r in result if r[3] == quad]
            warp_ids = [r[11] for r in result if r[3] == quad]
            visit_nums = [visit_nums[i] for i in good_idx]
            warp_ids = [warp_ids[i] for i in good_idx]
            sort_idx = sorted(range(len(visit_nums)), key=lambda k: visit_nums[k])
            visit_nums = [visit_nums[i] for i in sort_idx]
            warp_ids = [warp_ids[i] for i in sort_idx]
            visit_nums = [
                visit_nums[i] for i in range(len(visit_nums)) if warp_ids[i] is not None
            ]
            warp_ids = [warp_id for warp_id in warp_ids if warp_id is not None]
            if len(warp_ids) == 4:
                warp_id_pairs = [(warp_ids[0], warp_ids[1]), (warp_ids[2], warp_ids[3])]
            elif len(warp_ids) == 3:
                warp_id_pairs = [(warp_ids[0], warp_ids[1]), (warp_ids[1], warp_ids[2])]
            elif len(warp_ids) == 2:
                warp_id_pairs = [(warp_ids[0], warp_ids[1])]
            elif len(warp_ids) <= 1:
                warp_id_pairs = []
            if warp_id_pairs:
                for pair in warp_id_pairs:
                    difftool_cmd = [
                        "difftool",
                        "-dbname",
                        dbname,
                        "-definewarpwarp",
                        "-warp_id",
                        str(pair[0]),
                        "-template_warp_id",
                        str(pair[1]),
                        "-backwards",
                        "-set_workdir",
                        f"neb://@HOST@.0/{dbname}/ccl.ippreftest",
                        "-set_dist_group",
                        "NULL",
                        "-set_label",
                        label,
                        "-set_data_group",
                        label,
                        "-set_reduction",
                        "SWEETSPOT",
                        "-simple",
                        "-rerun",
                    ]  # , "-pretend"]
                    if pretend:
                        difftool_cmd.append("-pretend")
                    print(" ".join(difftool_cmd))
                    subprocess.run(difftool_cmd)
                    sleep(0.01)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Make wwdiffs for specified chunk on dateobs."
    )
    parser.add_argument(
        "--dbname",
        type=str,
        choices=["gpc1", "gpc2"],
        nargs="?",
        help="Which database/telescope to check",
    )
    parser.add_argument(
        "--chunk_name", type=str, nargs="?", help="Which chunk to check"
    )
    parser.add_argument("--label", type=str, nargs="?", help="Which label to check")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        nargs="?",
        help="The date of night to check. Format: YYYY-MM-DD UTC. Default: current UTC date.",
    )
    parser.add_argument(
        "--pretend",
        # type=bool,      setting type conflicts with store_true(false), even if they are compatible
        # default=False,  no needed when store_true
        action="store_true",
        # nargs="?",      conflicts with store_true; in this case the optional argument set a flag and does not accept input, while "?" still allows one input at most
        help="Elect to do pretend run. Default: False.",
    )
    parsed_args = parser.parse_args()
    # print(parsed_args)
    # start_time = time.time()
    make_wwdiff(
        chunk_name=parsed_args.chunk_name,
        dbname=parsed_args.dbname,
        dateobs=parsed_args.date,
        label=parsed_args.label,
        pretend=parsed_args.pretend,
    )
