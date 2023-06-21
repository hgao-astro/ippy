import sys

if sys.version_info[:2] >= (3, 7):
    from .mysql import NEBULOUS, NEBULOUS1, NEBULOUS2, SCIDBM, SCIDBS1, SCIDBS2
    from .pixels import GPC1, GPC2
else:
    from .mysql import *
    from .pixels import *
