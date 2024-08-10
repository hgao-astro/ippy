import datetime
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
    query = f"select class_id, uri from rawExp join rawImfile using (exp_id) where rawExp.exp_name like '{exp_name}'"
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


def find_active_detrend(type, time=None, filter=None, dbname="gpc1"):
    """
    find active detrend for a given time and type

    Parameters
    ----------
    type : str
    type of detrend, the common ones are "dark", "flat", or "mask"

    time : str or datetime.datetime
        time of the observation. default is None, which means the current UTC date.

    dbname : str, optional, should be gpc1 or gpc2. default is gpc1.

    Returns
    -------
    int
        det_id of the active detrend

    Raises
    ------
    TypeError
        when time is not a datetime.datetime object
    ValueError
        when type is not "dark" or "flat"
    """
    if time is None:
        time = datetime.datetime.utcnow()
    db_conn = MySQLdb.connect(
        host=SCIDBS1.node, db=dbname, user=SCIDBS1.user, passwd=SCIDBS1.password
    )
    db_cur = db_conn.cursor()
    query = f"""select det_id, iteration, det_type, mode, state, time_begin, time_end, use_begin, use_end 
    from detRun where det_type like "{type}" and state like 'stop' and (time_begin is NULL or time_begin  <= "{time}") 
    and (time_end is NULL or time_end >= "{time}") and (use_begin is NULL or use_begin <= "{time}") 
    and (use_end is NULL or use_end >= "{time}") 
    """
    if filter is not None:
        query += f" and filer like '{filter}'"
    query += " order by coalesce(time_begin, use_begin) desc"
    db_cur.execute(query)
    result = db_cur.fetchone()
    db_cur.close()
    db_conn.close()
    if result:
        return int(result[0])
    else:
        raise ValueError(
            f"Cannot find {dbname} active detrend for {time} and type {type} and filter {filter}."
        )


def find_detrend_imfile(det_id, ota=None, iteration=None, dbname="gpc1"):
    """
    find detrend files based on det_id

    Parameters
    ----------
    det_id : int
        det_id of the detrend

    ota : str or a list of str, optional

    dbname : str, optional, should be gpc1 or gpc2. default is gpc1.

    Returns
    -------
    str
        nebulous paths of detrend files

    Raises
    ------
    TypeError
        when det_id is not an integer
    ValueError
        when det_id is not a valid det_id
    """
    if not isinstance(det_id, int):
        det_id = int(det_id)
    db_conn = MySQLdb.connect(
        host=SCIDBS1.node, db=dbname, user=SCIDBS1.user, passwd=SCIDBS1.password
    )
    db_cur = db_conn.cursor()
    query = f"select class_id, uri, data_state, fault from  detRegisteredImfile where det_id = {det_id}"
    if iteration is not None:
        query += f" and iteration = {iteration}"
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
        # return a dictionary of ota as key and detrended image file paths as values
        return {r[0]: r[1] for r in result}
    else:
        raise ValueError(
            f"Cannot find detrended file for {dbname} det_id={det_id}, iteration={iteration}, and OTA={ota}."
        )


def cells_to_binary(cells, flag=True):
    """Converts a list of cells to a binary number.

    Args:
        cells (list): A list of cells in the format ["xy10", "xy23", "xy67"].

    Returns:
        str: A 64 bit binary number representing the list of cells. Each bit is zero (one) unless the corresponding cell is present, if flag is True (False).
        The output string is useful for per-cell configuration of applying pattern row correction or not.
    """
    if not isinstance(cells, list):
        cells = [cells]
    if not all(isinstance(cell, str) for cell in cells):
        raise ValueError("All elements in cells must be strings.")
    binary_number = 0
    for cell in cells:
        binary_number |= 1 << int(cell[2:], 8)
    # if flag is False, invert the binary number
    if not flag:
        binary_number = ~binary_number & 2**64 - 1
    return format(binary_number, "064b")[::-1]
