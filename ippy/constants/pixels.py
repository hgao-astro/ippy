CELL_NUM_PIXEL_ROW, CELL_NUM_PIXEL_COL = 598, 590
CHIP_NUM_PIXEL_ROW, CHIP_NUM_PIXEL_COL = 4868, 4846
CELL_NUM_PIXEL_ROW_GAP, CELL_NUM_PIXEL_COL_GAP = 12, 18
CELLS = tuple(f"xy{x}{y}" for x in range(8) for y in range(8))
NUM_CELL_PER_CHIP: int = len(CELLS)
OTAS_GPC2 = tuple(f"XY{x}{y}" for x in range(8) for y in range(8))
OTAS_GPC1 = list(OTAS_GPC2)
OTAS_GPC1.remove("XY00")
OTAS_GPC1.remove("XY07")
OTAS_GPC1.remove("XY70")
OTAS_GPC1.remove("XY77")
OTAS_GPC1 = tuple(OTAS_GPC1)

import sys

if sys.version_info[:2] >= (3, 7):
    from dataclasses import dataclass
    from typing import Tuple
    @dataclass(frozen=True)
    class Camera:
        name: str
        otas: Tuple[str,...]
        chip_num_pix_row = CHIP_NUM_PIXEL_ROW
        chip_num_pix_col = CHIP_NUM_PIXEL_COL
        cells = CELLS
        cell_num_pix_row = CELL_NUM_PIXEL_ROW
        cell_num_pix_col = CELL_NUM_PIXEL_COL
        cell_num_pix_row_gap = CELL_NUM_PIXEL_ROW_GAP
        cell_num_pix_col_gap = CELL_NUM_PIXEL_COL_GAP
        @property
        def num_chips(self):
            return len(self.otas)
        @property
        def num_cell_per_chip(self):
            return len(self.cells)
        @property
        def num_cell_per_row(self):
            xs = [int(cell[2]) for cell in self.cells]
            return max(xs) + 1
        @property
        def num_cell_per_col(self):
            ys = [int(cell[3]) for cell in self.cells]
            return max(ys) + 1

    GPC1 = Camera("GPC1", OTAS_GPC1)
    GPC2 = Camera("GPC2", OTAS_GPC2)

# if __name__ == "__main__":
#     print(GPC1)
#     print(GPC2)
#     print(GPC1.chip_num_pix_col)
#     print(GPC1.chip_num_pix_row)
#     print(GPC1.cell_num_pix_row, GPC1.cell_num_pix_col, GPC1.cell_num_pix_row_gap, GPC1.cell_num_pix_col_gap)
#     print(GPC1.num_chips())
#     print(GPC2.num_chips())
#     print(GPC1.num_cell_per_chip())
#     print(GPC2.num_cell_per_chip())

# MSKNAM00= 'DETECTOR'           / Bitmask bit name
# MSKVAL00=                    1 / Bitmask bit value
# MSKNAM01= 'FLAT    '           / Bitmask bit name
# MSKVAL01=                    2 / Bitmask bit value
# MSKNAM02= 'DARK    '           / Bitmask bit name
# MSKVAL02=                    4 / Bitmask bit value
# MSKNAM03= 'BLANK   '           / Bitmask bit name
# MSKVAL03=                    8 / Bitmask bit value
# MSKNAM04= 'CTE     '           / Bitmask bit name
# MSKVAL04=                   16 / Bitmask bit value
# MSKNAM05= 'SAT     '           / Bitmask bit name
# MSKVAL05=                   32 / Bitmask bit value
# MSKNAM06= 'LOW     '           / Bitmask bit name
# MSKVAL06=                   64 / Bitmask bit value
# MSKNAM07= 'SUSPECT '           / Bitmask bit name
# MSKVAL07=                  128 / Bitmask bit value
# MSKNAM08= 'BURNTOOL'           / Bitmask bit name
# MSKVAL08=                  128 / Bitmask bit value
# MSKNAM09= 'CR      '           / Bitmask bit name
# MSKVAL09=                  256 / Bitmask bit value
# MSKNAM10= 'SPIKE   '           / Bitmask bit name
# MSKVAL10=                  512 / Bitmask bit value
# MSKNAM11= 'GHOST   '           / Bitmask bit name
# MSKVAL11=                 1024 / Bitmask bit value
# MSKNAM12= 'STREAK  '           / Bitmask bit name
# MSKVAL12=                 2048 / Bitmask bit value
# MSKNAM13= 'STARCORE'           / Bitmask bit name
# MSKVAL13=                 4096 / Bitmask bit value
# MSKNAM14= 'CONV.BAD'           / Bitmask bit name
# MSKVAL14=                 8192 / Bitmask bit value
# MSKNAM15= 'CONV.POOR'          / Bitmask bit name
# MSKVAL15=                16384 / Bitmask bit value
# MSKNAM16= 'MASK.VALUE'         / Bitmask bit name
# MSKVAL16=                 8575 / Bitmask bit value
# MSKNAM17= 'MARK.VALUE'         / Bitmask bit name
# MSKVAL17=                32768 / Bitmask bit value
# MSKNUM  =                   18 / Bitmask bit count
