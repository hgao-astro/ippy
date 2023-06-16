import sys

from .mt_copy import mt_copy2

if sys.version_info[:2] >= (3, 7):
    from .read_fits import read_cell, read_chip
