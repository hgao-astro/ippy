SCIDBM = "scidbm"
SCIDBS1 = "scidbs1"
SCIDBS2 = "scidbs2"
MYSQL_USER_READ_ONLY = REDACTED_NORMAL
MYSQL_PSW_READ_ONLY = REDACTED_NORMAL
MYSQL_USER_POWER = REDACTED_POWER
MYSQL_PSW_POWER = REDACTED_POWER
NEBULOUS = "ippdb11"
NEBULOUS1 = "ippdb12"
NEBULOUS2 = "ippdb13"

import sys

if sys.version_info[:2] >= (3, 7):
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class MySQLHost:
        name: str
        node: str
        user: str
        password: str

    SCIDBM = MySQLHost("scidbm", "ippdb05", REDACTED_NORMAL, REDACTED_NORMAL)
    SCIDBS1 = MySQLHost("scidbs1", "ippdb01", REDACTED_NORMAL, REDACTED_NORMAL)
    SCIDBS2 = MySQLHost("scidbs2", "ippdb09", REDACTED_NORMAL, REDACTED_NORMAL)
    NEBULOUS = MySQLHost("nebulous", "ippdb11", REDACTED_POWER, REDACTED_POWER)
    NEBULOUS1 = MySQLHost("nebulous1", "ippdb12", REDACTED_POWER, REDACTED_POWER)
    NEBULOUS2 = MySQLHost("nebulous2", "ippdb13", REDACTED_POWER, REDACTED_POWER)
