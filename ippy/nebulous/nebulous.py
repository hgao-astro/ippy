import sys
from pathlib import Path

import MySQLdb

from ippy.io import mt_copy2

if sys.version_info[:2] >= (3, 7):
    from ippy.constants import NEBULOUS1

    NEBULOUS_HOST = NEBULOUS1.node
    NEBULOUS_USER = NEBULOUS1.user
    NEBULOUS_PSW = NEBULOUS1.password
else:
    from ippy.constants import MYSQL_PSW_POWER, MYSQL_USER_POWER, NEBULOUS1

    NEBULOUS_HOST = NEBULOUS1
    NEBULOUS_USER = MYSQL_USER_POWER
    NEBULOUS_PSW = MYSQL_PSW_POWER


def neb_locate(ext_id, no_wildcard=False):
    ext_id = str(ext_id).lower().strip()
    if ext_id.startswith("neb://"):
        ext_id = ext_id[6:]
        pos_slash = ext_id.find("/")
        if pos_slash != -1:
            ext_id = ext_id[pos_slash + 1 :]

    # strip characters before gpc1[2]
    # if not ext_id.startswith("gpc"):
    #     pos_gpc = ext_id.find("gpc")
    #     if pos_gpc != -1:
    #         ext_id = ext_id[pos_gpc:]
    #     else:
    #         raise ValueError(f"{ext_id} is not a valid Nebulous key.")
    # strip duplicate /
    ext_id_parts = ext_id.split("/")
    ext_id_parts = [p for p in ext_id_parts if p]  # remove empty substrings
    ext_id = "/".join(ext_id_parts)
    # deal with wildcards
    if no_wildcard:
        if "%" in ext_id:
            ext_id = ext_id.replace("%", "\%")
        if "_" in ext_id:
            ext_id = ext_id.replace("_", "\_")

    query = f"select ext_id, uri, name, allocate, available, xattr from storage_object left join instance using (so_id) left join volume using (vol_id) where ext_id like '{ext_id}'"
    neb_conn = MySQLdb.connect(
        host=NEBULOUS_HOST, db="nebulous", user=NEBULOUS_USER, passwd=NEBULOUS_PSW
    )
    neb_cur = neb_conn.cursor()
    neb_cur.execute(query)
    result = neb_cur.fetchall()
    neb_cur.close()
    neb_conn.close()
    if result:
        return [
            {
                "ext_id": r[0],
                "path": r[1].replace("file://", "") if r[1] else None,
                "volume": r[2],
                "allocate": r[3],
                "available": r[4],
                "xattr": r[5],
            }
            for r in result
        ]


def neb_replace(phy_path, neb_key, review=True, verbose=True):
    if not Path(phy_path).is_file():
        raise ValueError(f"{phy_path} is not a file.")
    instances = neb_locate(neb_key)
    if instances:
        if verbose:
            print(f"Found {len(instances)} instances of {neb_key} in Nebulous:")
            for instance in instances:
                print(f"Replace {instance['path']} with {phy_path}")
        if review:
            answer = input("Are you sure that you want to proceed? answer with y/n: ")
            if answer.lower() == "y":
                mt_copy2(phy_path, [instance["path"] for instance in instances])
            else:
                print("Nothing has been done.")
        else:
            mt_copy2(phy_path, [instance["path"] for instance in instances])
    else:
        print(f"{neb_key} not found in Nebulous.")
