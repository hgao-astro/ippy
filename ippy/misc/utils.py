import re

import MySQLdb

expname_pattern = re.compile(r"^[oc]\d{4,5}[gh]\d{4}[obdfl]$")
gpc1_expname_pattern = re.compile(r"^[oc]\d{4,5}g\d{4}[obdfl]$")
gpc2_expname_pattern = re.compile(r"^[oc]\d{4,5}h\d{4}[obdfl]$")

from ippy.constants import SCIDBS1


def infer_inst_from_expname(expname):
    """
    infer instrument based on exposure name format

    Parameters
    ----------
    expname : str
        exposure name

    Returns
    -------
    str
        intrument name, either "gpc1" or "gpc2".

    Raises
    ------
    TypeError
        when expname is not a string
    ValueError
        when expname is not a valid exposure name
    """
    if isinstance(expname, str):
        expname = expname.strip()
    else:
        raise TypeError(f"expname must be a string, not {type(expname)}.")
    if gpc1_expname_pattern.match(expname):
        return "gpc1"
    elif gpc2_expname_pattern.match(expname):
        return "gpc2"
    else:
        raise ValueError(f"Invalid exposure name: {expname}")


def find_raw_imfile(exp_name, ota=None):
    """
    find raw image file based on exposure name

    Parameters
    ----------
    exp_name : str
        exposure name

    ota : str or a list of str, optional

    Returns
    -------
    str
        nebulous paths of raw image files

    Raises
    ------
    TypeError
        when expname is not a string
    ValueError
        when expname is not a valid exposure name
    """
    if isinstance(exp_name, str):
        dbname = infer_inst_from_expname(exp_name)
    else:
        raise TypeError(f"exp_name must be a string, not {type(exp_name)}.")
    db_conn = MySQLdb.connect(
        host=SCIDBS1.node, db=dbname, user=SCIDBS1.user, passwd=SCIDBS1.password
    )
    db_cur = db_conn.cursor()
    query = f"select class_id, uri, fault, quality from rawImfile where exp_name like '{exp_name}'"
    if ota is not None:
        if isinstance(ota, str):
            query += f" and class_id like '{ota}'"
        elif isinstance(ota, list) and all(isinstance(o, str) for o in ota):
            # make sure ota has more than one element otherwise tuple(ota) will be (ota[0],) and break the query
            ota = ota + [ota[-1]]
            query += f" and class_id in {tuple(ota)}"
        else:
            raise TypeError(f"ota must be a string or a list of strings.")
    db_cur.execute(query)
    result = db_cur.fetchall()
    db_cur.close()
    db_conn.close()
    if result:
        # return a dictionary of ota as key and raw image file paths as values
        return {r[0]: r[1] for r in result}
    else:
        raise ValueError(f"Cannot find raw image file for {exp_name} and OTA {ota}.")
