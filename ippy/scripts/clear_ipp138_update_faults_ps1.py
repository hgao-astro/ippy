import argparse
import re
import subprocess
import sys
from pathlib import Path
from time import sleep

import MySQLdb

ippy_parent_dir = str(Path(__file__).resolve().parents[2])
if ippy_parent_dir not in sys.path:
    sys.path.append(ippy_parent_dir)

from ippy.nebulous import neb_locate

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


def tail(path, nline=50):
    with open(path, "r") as f:
        lines = f.readlines()
        return lines[-nline:]


def find_missing_nebkey(log_neb_path):
    log_phy_path = neb_locate(log_neb_path)
    if (
        log_phy_path is not None
        and len(log_phy_path) == 1
        and log_phy_path[0]["available"]
    ):
        log_path = log_phy_path[0]["path"]
    else:
        print(f"Update log {log_neb_path} is not available")
        return None
    log_cont = tail(log_path)
    log_cont.reverse()
    err_msg = re.compile(
        r"couldn't find input|failed to (open|read)", flags=re.IGNORECASE
    )
    neb_key = re.compile(r"\s(neb://\S+)\b")
    for line in log_cont:
        if err_msg.search(line):
            m = neb_key.search(line)
            if m:
                return m.group(1)
            else:
                return None
    else:
        return None


def classify_problem(missing_nebkey, log_neb_path):
    if missing_nebkey is None:
        return missing_nebkey, None
    # define various nebulous key patterns
    missing_warp_product = re.compile(
        r"^neb://\S+\.wrp\.\d+\.skycell\.\d+\.\d+\.\S*(fits|cmf)$"
    )
    missing_subkernel = re.compile(
        r"^neb://\S+\.skycell\.\d+\.\d+(\.WS)?\.dif\.\d+\.subkernel$"
    )
    # missing_subkernel_gone = re.compile(
    #     r"^neb://\S+\.skycell\.\d+\.\d+(\.WS)?\.dif\.\d+\.subkernel\.GONE$"
    # )
    missing_stk_cmf = re.compile(r"^neb://\S+\.skycell\.\d+\.\d+\.stk\.(\d+)\.cmf$")
    missing_mdl = re.compile(r"^neb://\S+\.skycell\.\d+\.\d+\.WS\.dif\.\d+\.mdl\.fits$")
    # match the missing nebulous key with the pattern
    # various warp products, including IPP-1762; see https://panstarrs.atlassian.net/wiki/spaces/IPPCZAR/pages/678633973/ipp138.0+Warp+Diff+Data+Recovery#Missing-wrp.skycell.fits-file-SOLVED
    if missing_warp_product.fullmatch(missing_nebkey):
        culprit_phy_path = neb_locate(missing_nebkey)
        if (
            culprit_phy_path is not None
            and len(culprit_phy_path) >= 1
            and not culprit_phy_path[0]["available"]
            and culprit_phy_path[0]["volume"].startswith("ipp138.0")
        ):
            return missing_nebkey, repair_warp
        else:
            return missing_nebkey, None
    # IPP-1776
    if missing_stk_cmf.fullmatch(missing_nebkey):
        culprit_phy_path = neb_locate(missing_nebkey)
        if culprit_phy_path is None:
            m = missing_stk_cmf.fullmatch(missing_nebkey)
            stack_id = m.group(1)
            query = f"""
            select stack_id, hostname from stackSumSkyfile where stack_id={stack_id}
            """
            db_conn = MySQLdb.connect(
                host=SCIDBS1_HOST, db="gpc1", user=SCIDBS1_USER, passwd=SCIDBS1_PSW
            )
            db_cursor = db_conn.cursor()
            db_cursor.execute(query)
            result = db_cursor.fetchone()
            db_cursor.close()
            db_conn.close()
            if result is not None:
                stack_id, hostname = result
                if hostname.strip() == "LANL/Mustang":
                    # at this point we are confident that it is the offsite LANL HPC processing situation (IPP-1776)
                    # parse the log again
                    log_phy_path = neb_locate(log_neb_path)
                    if (
                        log_phy_path is not None
                        and len(log_phy_path) == 1
                        and log_phy_path[0]["available"]
                    ):
                        log_path = log_phy_path[0]["path"]
                    else:
                        print(f"Update log {log_neb_path} is not available")
                        return None
                    log_cont = tail(log_path)
                    log_cont.reverse()
                    # locate the actually culprit: warp cmf file
                    neb_key = re.compile(
                        r"^inputSources:\s(neb://\S+\.wrp\.\d+\.skycell\.\d+\.\d+\.cmf)\b"
                    )
                    for line in log_cont:
                        if neb_key.search(line):
                            m = neb_key.search(line)
                            missing_nebkey_ = m.group(1)
                            culprit_phy_path = neb_locate(missing_nebkey_)
                            if (
                                culprit_phy_path is not None
                                and len(culprit_phy_path) >= 1
                                and not culprit_phy_path[0]["available"]
                                and culprit_phy_path[0]["volume"].startswith("ipp138.0")
                            ):
                                return missing_nebkey_, repair_warp
                            else:
                                return missing_nebkey, None
                    else:
                        return missing_nebkey, None
            else:
                return missing_nebkey, None
        else:
            return missing_nebkey, None
    # IPP-1580 or IPP-1826
    # IPP-1826 is a special case when the missing file is not neccessarily in ipp138.0
    # the problem probably arise from experiments trying to fix IPP-1580
    # I include it here because it has a fairly simple solution
    # and it is a hassele to manually fix it when there are many of faulted items
    if missing_subkernel.fullmatch(missing_nebkey):
        culprit_phy_path = neb_locate(missing_nebkey)
        if culprit_phy_path is None:
            subkernel_gone_guess = neb_locate(missing_nebkey + ".GONE")
            if subkernel_gone_guess is not None and len(subkernel_gone_guess) >= 1:
                return missing_nebkey, mv_subkernel_gone
            else:
                return missing_nebkey, None
        else:
            if (
                len(culprit_phy_path) == 1
                and not culprit_phy_path[0]["available"]
                and culprit_phy_path[0]["volume"].startswith("ipp138.0")
            ):
                return missing_nebkey, "ipp1580"
            else:
                return missing_nebkey, None
    # IPP-1606
    if missing_mdl.fullmatch(missing_nebkey):
        culprit_phy_path = neb_locate(missing_nebkey)
        if (
            culprit_phy_path is not None
            and len(culprit_phy_path) == 1
            and not culprit_phy_path[0]["available"]
            and culprit_phy_path[0]["volume"].startswith("ipp138.0")
        ):
            return missing_nebkey, "ipp1606"
        else:
            return missing_nebkey, None


def clear_faults(diff_id, skycell_ids, label, faults, pretend=True):
    if not isinstance(diff_id, int) or not isinstance(label, str):
        raise ValueError("diff_id and label must be a scalar")
    if len(skycell_ids) != len(faults):
        raise ValueError("The length of skycell_ids and faults must be the same")
    # not setting fault 5 to skycells already with fault 5 because it will throw an error saying failed to set set fault flag
    skycell_ids_ = [
        skycell_id for idx, skycell_id in enumerate(skycell_ids) if faults[idx] != 5
    ]
    change_label = [
        "difftool",
        "-dbname",
        "gpc1",
        "-updaterun",
        "-set_label",
        label,
        "-diff_id",
        str(diff_id),
    ]
    set_fault_5 = [
        "difftool",
        "-dbname",
        "gpc1",
        "-updatediffskyfile",
        "-fault",
        "5",
        "-diff_id",
        str(diff_id),
    ]
    if pretend:
        print("Suggested commands:")
        print(" ".join(change_label))
        if len(skycell_ids_) < len(skycell_ids):
            print(f"Skipped {len(skycell_ids)-len(skycell_ids_)} skycell with fault 5.")
        for skycell_id in skycell_ids_:
            print(" ".join(set_fault_5 + ["-skycell_id", skycell_id]))
    else:
        print("Running commands ...")
        print(" ".join(change_label))
        try:
            subprocess.run(
                change_label,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
        except subprocess.CalledProcessError as e:
            print(
                f"Command '{' '.join(e.cmd)}' returned non-zero exit status, please check its stderr below."
            )
            # print(e.stdout)
            print(e.stderr)
            # return None
        sleep(0.01)
        if len(skycell_ids_) < len(skycell_ids):
            print(f"Skipped {len(skycell_ids)-len(skycell_ids_)} skycell with fault 5.")
        for skycell_id in skycell_ids_:
            cmd = set_fault_5 + ["-skycell_id", skycell_id]
            print(" ".join(cmd))
            try:
                subprocess.run(
                    cmd,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
            except subprocess.CalledProcessError as e:
                print(
                    f"Command '{' '.join(e.cmd)}' returned non-zero exit status, please check its stderr below."
                )
                # print(e.stdout)
                print(e.stderr)
                # return None
            sleep(0.01)


def mv_subkernel_gone(missing_nebkey, pretend=True):
    """
    solution to IPP-1826

    Parameters
    ----------
    missing_nebkey : str
        nebulous key of the missing file
    pretend : bool, optional
        only print the command not execute it, by default True
    """
    neb_mv = ["neb-mv", missing_nebkey + ".GONE", missing_nebkey]
    if pretend:
        print("Suggested command:")
        print(" ".join(neb_mv))
    else:
        print("Running command:")
        print(" ".join(neb_mv))
        try:
            subprocess.run(
                neb_mv,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
        except subprocess.CalledProcessError as e:
            print(
                f"Command '{' '.join(e.cmd)}' returned non-zero exit status, please check its stderr below."
            )
            # print(e.stdout)
            print(e.stderr)


def repair_warp(missing_nebkey, pretend=True):
    missing_warp_product = re.compile(
        r"^neb://\S+\.wrp\.(\d+)\.(skycell\.\d+\.\d+)\.\S*(fits|cmf)$"
    )
    m = missing_warp_product.fullmatch(missing_nebkey)
    if m is None:
        print("missing_nebkey is not a warp product")
        return None
    warp_id = m.group(1)
    skycell_id = m.group(2)
    # query for chip_id and chipRun.state to determine whether to update chips or not
    query = f"""
    select chip_id, chipRun.state, chipRun.label, warp_id, warpRun.state, warpRun.label 
    from chipRun
    left join camRun using (chip_id) 
    left join fakeRun using (cam_id) 
    left join warpRun using (fake_id) 
    where warp_id = {warp_id}
    """
    db_conn = MySQLdb.connect(
        host=SCIDBS1_HOST, db="gpc1", user=SCIDBS1_USER, passwd=SCIDBS1_PSW
    )
    db_cursor = db_conn.cursor()
    db_cursor.execute(query)
    result = db_cursor.fetchone()
    db_cursor.close()
    db_conn.close()
    if result is not None:
        chip_id = result[0]
        chip_state = result[1]
        # chip_label = result[2]
        # warp_id = result[3]
        # warp_state = result[4]
        # warp_label = result[5]
    else:
        return None
    clean_skycell = [
        "warptool",
        "-dbname",
        "gpc1",
        "-tocleanedskyfile",
        "-warp_id",
        warp_id,
        "-skycell_id",
        skycell_id,
    ]
    clean_warp = [
        "warptool",
        "-dbname",
        "gpc1",
        "-updaterun",
        "-set_state",
        "cleaned",
        "-set_label",
        "goto_cleaned",
        "-warp_id",
        warp_id,
    ]
    update_warp = [
        "warptool",
        "-dbname",
        "gpc1",
        "-setskyfiletoupdate",
        "-set_label",
        "update.OSS",
        "-warp_id",
        warp_id,
    ]
    if chip_state != "full":
        update_chip = [
            "chiptool",
            "-dbname",
            "gpc1",
            "-setimfiletoupdate",
            "-set_label",
            "update.OSS",
            "-chip_id",
            str(chip_id),
        ]
        if pretend:
            # print(chip_id, chip_state, chip_label)
            print("Suggested commands:")
            print(" ".join(update_chip))
        else:
            print("Running commands ...")
            print(" ".join(update_chip))
            try:
                subprocess.run(
                    update_chip,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
            except subprocess.CalledProcessError as e:
                print(
                    f"Command '{' '.join(e.cmd)}' returned non-zero exit status, please check its stderr below."
                )
                # print(e.stdout)
                print(e.stderr)
                return None
    if pretend:
        # print(warp_id, warp_state, warp_label)
        if chip_state == "full":
            print("Suggested commands:")
        print(" ".join(clean_skycell))
        print(" ".join(clean_warp))
        print(" ".join(update_warp))
    else:
        if chip_state == "full":
            print("Running commands ...")
        print(" ".join(clean_skycell))
        print(" ".join(clean_warp))
        print(" ".join(update_warp))
        try:
            subprocess.run(
                clean_skycell,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            sleep(0.05)
            subprocess.run(
                clean_warp,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            sleep(0.05)
            subprocess.run(
                update_warp,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
        except subprocess.CalledProcessError as e:
            print(
                f"Command '{' '.join(e.cmd)}' returned non-zero exit status, please check its stderr below."
            )
            # print(e.stdout)
            print(e.stderr)
            return None


def main(label, pretend=True, limit=None):
    if label == "all":
        label = "ps_ud_%"
    elif not label.startswith("ps_ud_"):
        print("label must start with 'ps_ud_'")
        return
    query = f"""
    select diff_id, skycell_id, label, concat(path_base,".log.update"), fault 
    from diffRun left join diffSkyfile using (diff_id)  
    where diffSkyfile.fault != 0
    and data_state like "update" 
    and label like "{label}" 
    and label not like "%ipp%" 
    and label not like "%broke%"
    and label not like "%clean%"
    and label not like "%hold%"
    """
    if limit is not None:
        query += f" limit {limit}"
    db_conn = MySQLdb.connect(
        host=SCIDBS1_HOST, db="gpc1", user=SCIDBS1_USER, passwd=SCIDBS1_PSW
    )
    db_cursor = db_conn.cursor()
    db_cursor.execute(query)
    result = db_cursor.fetchall()
    db_cursor.close()
    db_conn.close()
    if result is None:
        return None
    diff_ids = []
    skycell_ids = []
    labels = []
    missing_nebkeys = []
    solutions = []
    faults = []
    for item in result:
        # print(item[:])
        missing_nebkey = find_missing_nebkey(item[3])
        if missing_nebkey is None:
            continue
        # culprit_phy_path = neb_locate(missing_nebkey)
        missing_nebkey, solution = classify_problem(missing_nebkey, item[3])
        if solution is None:
            continue
        else:
            diff_ids.append(item[0])
            skycell_ids.append(item[1])
            labels.append(item[2])
            missing_nebkeys.append(missing_nebkey)
            solutions.append(solution)
            faults.append(item[4])
    diff_ids_to_fix = []
    # skycell_ids_to_fix = []
    # missing_nebkeys_to_fix = []
    # labels_to_fix = []
    diff_ids_to_clear = []
    skycell_ids_to_clear = []
    missing_nebkeys_to_clear = []
    labels_to_clear = []
    faults_to_clear = []
    label_suffixes = []
    # fix faults with known solutions first
    for idx, solution in enumerate(solutions):
        # fixable faults
        if callable(solution):
            diff_ids_to_fix.append(diff_ids[idx])
            # skycell_ids_to_fix.append(skycell_ids[idx])
            # missing_nebkeys_to_fix.append(missing_nebkeys[idx])
            # labels_to_fix.append(labels[idx])
            print(
                f"diff_id={diff_ids[idx]}, skycell_id={skycell_ids[idx]}, fault={faults[idx]}, missing_nebkey={missing_nebkeys[idx]}, solution={solution.__name__}"
            )
            solution(missing_nebkeys[idx], pretend=pretend)
        # non-fixable faults, solution is a string of the JIRA ticket
        elif solution is not None and isinstance(solution, str):
            diff_ids_to_clear.append(diff_ids[idx])
            skycell_ids_to_clear.append(skycell_ids[idx])
            missing_nebkeys_to_clear.append(missing_nebkeys[idx])
            labels_to_clear.append(labels[idx])
            label_suffixes.append(solution)
            faults_to_clear.append(faults[idx])
    # then clear faults
    # if len(set(diff_ids_to_clear)) < len(diff_ids_to_clear):
    # group skycells of the same diff_id to avoid duplicate change label operations
    # also make sure the new label encapsulates all tickets of the skycell issues, e.g., ps_ud_QUB.ipp1580.ipp1762
    diff_ids_to_clear_uniq = sorted(set(diff_ids_to_clear))
    for diff_id in diff_ids_to_clear_uniq:
        idxs = [
            i for i in range(len(diff_ids_to_clear)) if diff_ids_to_clear[i] == diff_id
        ]
        skycell_ids_to_clear_ = []
        label_suffixes_ = []
        faults_to_clear_ = []
        # root of new label, i.e., the original label
        new_label = labels_to_clear[idxs[0]]
        for idx in idxs:
            print(
                f"diff_id={diff_ids_to_clear[idx]}, skycell_id={skycell_ids_to_clear[idx]}, fault={faults_to_clear[idx]}, missing_nebkey={missing_nebkeys_to_clear[idx]}, solution={label_suffixes[idx]}"
            )
            skycell_ids_to_clear_.append(skycell_ids_to_clear[idx])
            label_suffixes_.append(label_suffixes[idx])
            faults_to_clear_.append(faults_to_clear[idx])
        label_suffixes_ = set(label_suffixes_)
        new_label = new_label + "." + ".".join(label_suffixes_)
        # see if there are fixable skycells that belong the same diff_id
        if diff_id in diff_ids_to_fix:
            print(
                "clearing faults of the above skycells are blocked until fixable faults of other skycells that belong to the same diff_id are fixed"
            )
        else:
            clear_faults(
                diff_id,
                skycell_ids_to_clear_,
                new_label,
                faults_to_clear_,
                pretend=pretend,
            )
    # else:
    #     for diff_id, skycell_id, old_lable, label_suffix, missing_nebkey in zip(
    #         diff_ids_to_clear,
    #         skycell_ids_to_clear,
    #         labels_to_clear,
    #         label_suffixes,
    #         missing_nebkeys_to_clear,
    #     ):
    #         print(
    #             f"diff_id={diff_id}, skycell_id={skycell_id}, missing_nebkey={missing_nebkey}, solution={label_suffix}"
    #         )
    #         if diff_id in diff_ids_to_fix:
    #             print(
    #                 "clearing faults of the above skycells are blocked until fixable faults of the other skycells are fixed"
    #             )
    #         else:
    #             clear_faults(
    #                 diff_id,
    #                 [skycell_id],
    #                 old_lable + "." + label_suffix,
    #                 pretend=pretend,
    #             )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clear gpc1 ipp138.0 related update faults."
    )
    parser.add_argument(
        "--label",
        type=str,
        default="all",
        nargs="?",
        help="Which label to check and clear. Default is to check all ps_ud_%% labels.",
    )
    parser.add_argument(
        "--pretend",
        # type=bool,      setting type conflicts with store_true(false), even if they are compatible
        # default=False,  no needed when store_true
        action="store_true",
        # nargs="?",      conflicts with store_true; in this case the optional argument set a flag and does not accept input, while "?" still allows one input at most
        help="check only and not do anything. Default: False when not the flag specified.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        nargs="?",
        help="Limits of skycell_ids to check and clear. Useful for diagnose the issue before batch operations. Default is None for no limits.",
    )
    parsed_args = parser.parse_args()
    main(
        label=parsed_args.label,
        pretend=parsed_args.pretend,
        limit=parsed_args.limit,
    )
