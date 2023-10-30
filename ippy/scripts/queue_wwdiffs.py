#!/usr/bin/env python3

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import MySQLdb

ippy_parent_dir = str(Path(__file__).resolve().parents[2])
if ippy_parent_dir not in sys.path:
    sys.path.append(ippy_parent_dir)

from ippy.processing import Chunk

if sys.version_info[:2] >= (3, 7):
    from ippy.constants import SCIDBS1

    SCIDBS1_HOST = SCIDBS1.node
    SCIDBS1_USER = SCIDBS1.user
    SCIDBS1_PSW = SCIDBS1.password
else:
    from ippy.constants import MYSQL_PSW_READ_ONLY, MYSQL_USER_READ_ONLY, SCIDBS1

    SCIDBS1_HOST = SCIDBS1
    SCIDBS1_USER = MYSQL_USER_READ_ONLY
    SCIDBS1_PSW = MYSQL_PSW_READ_ONLY


def get_chunk_and_dateobs(dbname, label, data_group, chunks, dateobs):
    query = f"""
        select exp_name, exp_id, date(dateobs) dateobs1, substring_index(comment,' ',1) chunk_name, chipRun.label
        from rawExp 
        left join chipRun using (exp_id)
        left join camRun using (chip_id) 
        left join camProcessedExp using (cam_id)
        left join fakeRun using (cam_id) 
        left join warpRun using (fake_id) 
        where chipRun.label like "{label}"
        """
    if data_group is not None:
        query += f'and chipRun.data_group like "{data_group}"'
    if chunks is not None and dateobs is not None:
        query += " and ("
        for idx, chunk in enumerate(chunks):
            if idx != 0:
                query += f' or comment like "{chunk}% visit _"'
            else:
                query += f' comment like "{chunk}% visit _"'
        query += ")"
    query += " group by chunk_name, dateobs1 order by dateobs1"
    # print(query)
    db_conn = MySQLdb.connect(
        host=SCIDBS1_HOST,
        db=dbname,
        user=SCIDBS1_USER,
        passwd=SCIDBS1_PSW,
    )
    db_cursor = db_conn.cursor()
    db_cursor.execute(query)
    result = db_cursor.fetchall()
    db_cursor.close()
    db_conn.close()
    if result:
        chunks_from_db = [r[3] for r in result]
        dateobs_from_db = [r[2].strftime("%Y-%m-%d") for r in result]
        chunk_dateobs_pairs_from_db = set(zip(chunks_from_db, dateobs_from_db))
    else:
        return None
    if chunks is not None and dateobs is not None:
        # check if chunks and dateobs are of the same length
        if len(chunks) != len(dateobs):
            raise ValueError(f"chunks and dateobs must be of the same length.")
        # check if chunks and dateobs overlap with chunks and dateobs from the database
        chunk_dateobs_pairs = set(zip(chunks, dateobs))
        if not chunk_dateobs_pairs.issubset(chunk_dateobs_pairs_from_db):
            raise ValueError(
                f"Input chunks {chunks} on dateobs {dateobs} not found in the database for label {label}."
            )
        else:
            chunk_dateobs_pairs_from_db = chunk_dateobs_pairs
    return chunk_dateobs_pairs_from_db


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Queue WWdiffs for given dbname, label, and optionaly specified chunks. 
        if chunks are not specified, all chunks processed with the given label will be considered. 
        This script should be called after chip to warp processing have been queued using e.g. run_chiptool.py."""
    )
    parser.add_argument(
        "dbname", choices=["gpc1", "gpc2"], help="Database or camera name."
    )
    parser.add_argument(
        "label",
        help="The label for query chip to warp processing. The same label will also be used to queue WWdiffs.",
    )
    parser.add_argument(
        "--check_interval",
        type=int,
        default=60,
        help="Time interval in units of seconds for scanning the database and queueing WWdiffs.",
    )
    parser.add_argument(
        "--data_group",
        help="The data_group for query chip to warp processing.",
    )
    parser.add_argument(
        "--chunks",
        type=str,
        nargs="+",
        help="A list of chunk names separated by space.",
    )
    parser.add_argument(
        "--dateobs",
        type=str,
        nargs="+",
        help="A list of dateobs that correspond to the chunk names separated by space. Must be of the same length as chunks.",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Commit to queue the processing. Default: False when the flag is not specified so difftool will run with -pretend.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print extra info when checking. Default: False when the flag is not specified so will only print details of chunk/quad when there are diffs to be queued.",
    )
    args = parser.parse_args()

    while True:
        print("#" * 120)
        print(
            "Checking chunk/quad status " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        chunk_dateobs_pair = get_chunk_and_dateobs(
            dbname=args.dbname,
            label=args.label,
            data_group=args.data_group,
            chunks=args.chunks,
            dateobs=args.dateobs,
        )
        if chunk_dateobs_pair is None:
            raise ValueError(f"No chunks found in the database for label {args.label}.")
        count_diffs_to_queue = 0
        for chunk, dateobs in chunk_dateobs_pair:
            chunk = Chunk(
                chunk_name=chunk,
                dbname=args.dbname,
                dateobs=dateobs,
                label=args.label,
                data_group=args.data_group,
            )
            # check if any quads have more than 2 copy of the same visit
            # that suggests the chunk/quad have been processed with the same label more than once
            # need extra info to locate exactly the chunk/quad that needs wwdiffs, e.g., data_group
            # note that sometimes a quad may have more than four visits when there is an overridden visit, so use 8 here
            if any(len(quad.visits) >= 8 for quad in chunk.quads):
                raise ValueError(
                    f"{chunk.chunk_name} has been processed with the same label for more than once. Needs extra info to locate the chunk/quad. Please try supplying data_group."
                )
            print("=" * 120)
            print(chunk)
            for quad in chunk.quads:
                (
                    count_diffs_to_queue_this_quad,
                    count_diffs_queued_this_quad,
                ) = quad.queue_wwdiffs(pretend=True, verbose=False)
                count_diffs_to_queue += count_diffs_to_queue_this_quad
                if args.verbose or count_diffs_queued_this_quad > 0:
                    print("-" * 120)
                    print(quad)
                    for visit in quad.visits:
                        print(visit)
                    for wwdiff in quad.wwdiffs:
                        print(wwdiff)
                quad.queue_wwdiffs(pretend=not args.commit, verbose=True)
        if count_diffs_to_queue == 0:
            print("No more WWdiffs to queue. Aborting the scanning.")
            break
        else:
            time.sleep(args.check_interval)
