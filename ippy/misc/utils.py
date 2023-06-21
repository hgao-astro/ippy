import re

expname_pattern = re.compile(r"^[oc]\d{4,5}[gh]\d{4}[obdfl]$")
gpc1_expname_pattern = re.compile(r"^[oc]\d{4,5}g\d{4}[obdfl]$")
gpc2_expname_pattern = re.compile(r"^[oc]\d{4,5}h\d{4}[obdfl]$")


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
