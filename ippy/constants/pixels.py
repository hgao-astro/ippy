CELLS = tuple(f"xy{x}{y}" for x in range(8) for y in range(8))
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
        otas: Tuple[str, ...]
        chip_num_pix_row: int
        chip_num_pix_col: int
        cells = CELLS
        cell_num_pix_row: int
        cell_num_pix_col: int
        cell_num_pix_row_untrimmed = 608
        cell_num_pix_col_untrimmed = 624
        cell_num_pix_row_gap: int
        cell_num_pix_col_gap: int

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

    GPC1 = Camera(
        name="GPC1",
        otas=OTAS_GPC1,
        chip_num_pix_row=4868,
        chip_num_pix_col=4846,
        cell_num_pix_row=598,
        cell_num_pix_col=590,
        cell_num_pix_row_gap=12,
        cell_num_pix_col_gap=18,
    )
    GPC2 = Camera(
        name="GPC2",
        otas=OTAS_GPC2,
        chip_num_pix_row=4870,
        chip_num_pix_col=4862,
        cell_num_pix_row=600,
        cell_num_pix_col=592,
        cell_num_pix_row_gap=10,
        cell_num_pix_col_gap=18,
    )
