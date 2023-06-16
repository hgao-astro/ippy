import argparse
import sys
# import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ippy_parent_dir = str(Path(__file__).resolve().parents[2])
if ippy_parent_dir not in sys.path:
    sys.path.append(ippy_parent_dir)

from ippy.processing import Night


def main(dateobs, dbname, buffer_time, scan_interval, check_overdone):
    if dbname == "both":
        dbname = ["gpc1", "gpc2"]
    else:
        dbname = [dbname]
    if dateobs is None:
        dateobs = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    cur_time = datetime.now(timezone.utc)
    if scan_interval is None:
        # if None, set scan_interval to be large enough (now - dateobs 00:00) so alerts will always be triggered
        scan_interval = cur_time - datetime.strptime(dateobs, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        scan_interval = timedelta(minutes=scan_interval)
    for db in dbname:
        night = Night(dbname=db, dateobs=dateobs)
        if night.chunks:
            banner1 = "#"*120
            banner2 = "-"*135
            chunks_not_done = [
                chunk
                for chunk in night.chunks
                if not chunk.chunk_name.startswith("XSS")
                and chunk.obs_status != "in progress"
                and chunk.not_done
            ]
            chunks_stalled = [
                chunk
                for chunk in chunks_not_done
                if (
                    not chunk.needs_desp_diff
                    and timedelta(minutes=(40 / 20 * len(chunk.quads) + buffer_time))
                    + scan_interval
                    > (cur_time - chunk.last_visit.dateobs)
                    >= timedelta(minutes=(40 / 20 * len(chunk.quads) + buffer_time))
                )
                or (
                    chunk.needs_desp_diff
                    and timedelta(minutes=100 + buffer_time) + scan_interval
                    > (cur_time - chunk.last_visit.dateobs)
                    >= timedelta(minutes=100 + buffer_time)
                )
            ]
            if chunks_stalled:
                print(banner1)
                print(f"{db.upper()} stalled chunks:")
                for chunk in chunks_stalled:
                    time_since_chunk_finish = cur_time - chunk.last_visit.dateobs
                    time_since_chunk_finish_minute = int(
                        time_since_chunk_finish / timedelta(minutes=1)
                    )
                    print(banner2)
                    print(
                        f"{chunk} \n {time_since_chunk_finish_minute} minutes since the last exposure."
                    )
                    partially_done_quads = chunk.select_quads(partially_processed=True)
                    print("partially processed quads:")
                    for quad in partially_done_quads:
                        if quad.needs_desp_diff:
                            # if time_since_chunk_finish_minute <= 90:
                            #     print(
                            #         f"{quad} needs desperate diff. ETA {90-time_since_chunk_finish_minute} minutes."
                            #     )
                            # else:
                            print(f"{quad} desperate diff should be queued {time_since_chunk_finish_minute-90} minutes ago.")
                        else:
                            print(quad)
                        for v in quad.visits:
                            print(v)
                        for diff in quad.wwdiffs:
                            print(diff)
            # print(check_overdone)
            if check_overdone:
                chunks_over_done = [
                    chunk for chunk in night.chunks if chunk.over_done
                ]
                if chunks_over_done:
                    print(banner1)
                    print(f"{db.upper()} over processed chunks:")
                    for chunk in chunks_over_done:
                        print(banner2)
                        print(chunk)
                        over_done_quads = chunk.select_quads(over_processed=True)
                        print("over processed quads:")
                        for quad in over_done_quads:
                            print(quad)
                            for v in quad.visits:
                                print(v)
                            for diff in quad.wwdiffs:
                                print(diff)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check the progress of processing for chunks on a specified night."
    )
    parser.add_argument(
        "--dbname",
        type=str,
        default="both",
        choices=["gpc1", "gpc2", "both"],
        # nargs="?",
        help="Which database/telescope to check. Default is to check both gpc1 and gpc2.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        # nargs="?",
        help="The date of night to check. Format: YYYY-MM-DD UTC. Default: current UTC date.",
    )
    parser.add_argument(
        "--buffer",
        type=int,
        default=40,
        # nargs="?",
        help="""Buffer time in units of minutes to trigger alerts for the stalled chunks.
        The condition for triggering alerts is when time since last exposure of the chunk is
        larger than the estimated needed processing time plus the buffer. The estimated needed
        processing time is scaled by number of quads in the chunk nquads/20*40 for chunks that
        do not need desperate diffs; for chunks that need desperate diffs that would be 
        1 hr 30 minutes + processing of desperate diffs + buffer for desperate diffs and I 
        choose 100 minutes + buffer. Default: 40.""",
    )
    parser.add_argument(
        "--scan_interval",
        type=int,
        default=None,
        # nargs="?",
        help="Time interval for nightly checking cron jobs. It is needed to prevent more than one alert per chunk for nightly repeated checks. Default is None for one-time checks.",
    )
    parser.add_argument(
        "--check_overdone",
        # type=bool,      setting type conflicts with store_true(false), even if they are compatible
        # default=False,  no needed when store_true
        action="store_true",
        # nargs="?",      conflicts with store_true; in this case the optional argument set a flag and does not accept input, while "?" still allows one input at most
        help="Elect to check for over processed chunks. Default: False."
    )
    parsed_args = parser.parse_args()
    # print(parsed_args)
    # start_time = time.time()
    main(
        dbname=parsed_args.dbname,
        dateobs=parsed_args.date,
        buffer_time=parsed_args.buffer,
        scan_interval=parsed_args.scan_interval,
        check_overdone=parsed_args.check_overdone
    )
    # print(time.time() - start_time)
