from pathlib import Path

import numpy as np
from astropy.io.fits import HDUList
from astropy.visualization import ImageNormalize, ZScaleInterval
from matplotlib import pyplot as plt

from ippy.constants import GPC1, GPC2
from ippy.nebulous import neb_locate


def read_chip(data, mask=None):
    path = Path(data).expanduser()
    # check if the FITS file exists
    if not path.is_file():
        # check if it is a nebulous path, use the original path because Path(data) will remove extra // in the path
        if neb_locate(data):
            path = neb_locate(data)[0]["path"]
        else:
            raise FileNotFoundError(f"No such file: '{str(data)}'")
    chip_hdul = ChipHDUList.fromfile(path, mode="readonly")
    chip_hdul[1].header["extname"] = "data"
    telescope = chip_hdul[1].header.get("TELESCOP")
    instrument = chip_hdul[1].header.get("INSTRUME")
    if telescope == "PS1" or instrument == "gpc1":
        chip_hdul.camera = GPC1
    elif telescope == "PS2" or instrument == "gpc2":
        chip_hdul.camera = GPC2
    else:
        chip_hdul.camera = GPC1
    chip_hdul.telescope = telescope
    chip_hdul.instrument = instrument
    if mask is not None:
        chip_hdul.add_mask(mask)
    return chip_hdul


def read_cell(data, mask=None, trim_overscan=True):
    path = Path(data).expanduser()
    # check if the FITS file exists
    if not path.is_file():
        # check if it is a nebulous path
        if neb_locate(data):
            path = neb_locate(data)[0]["path"]
        else:
            raise FileNotFoundError(f"No such file: '{str(data)}'")
    cell_hdul = CellHDUList.fromfile(path, mode="readonly")
    telescope = cell_hdul[1].header.get("TELESCOP")
    instrument = cell_hdul[1].header.get("INSTRUME")
    if telescope == "PS1" or instrument == "gpc1":
        cell_hdul.camera = GPC1
    elif telescope == "PS2" or instrument == "gpc2":
        cell_hdul.camera = GPC2
    else:
        cell_hdul.camera = GPC1
    cell_hdul.telescope = telescope
    cell_hdul.instrument = instrument
    cell_hdul.trim_overscan = trim_overscan
    if cell_hdul.trim_overscan:
        for hdu in cell_hdul[1:]:
            if hdu.data.shape == (
                cell_hdul.camera.cell_num_pix_row_untrimmed,
                cell_hdul.camera.cell_num_pix_col_untrimmed,
            ):
                hdu.data = hdu.data[
                    : cell_hdul.camera.cell_num_pix_row,
                    : cell_hdul.camera.cell_num_pix_col,
                ]
    if mask is not None:
        cell_hdul.add_mask(mask)
    return cell_hdul


class ChipHDUList(HDUList):
    """
    HDUList subclass for handling chip fits file that ends with ota.ch.[mk.]fits. When the chip fits file is opened, a `ChipHDUList` object is returned.

    Args:
        HDUList (class `astropy.io.fits.HDUList`): HDU list class from astropy. Top-level FITS object.
    """

    def add_mask(self, mask_path):
        if "mask" in [hdu.name for hdu in self]:
            raise ValueError("Mask already exists")
        mask_path = Path(mask_path)
        if not mask_path.is_file():
            # check if it is a nebulous path
            if neb_locate(mask_path):
                mask_path = neb_locate(mask_path)[0]["path"]
            else:
                raise FileNotFoundError(f"No such file: '{str(mask_path)}'")
        # with ChipHDUList.fromfile(mask_path, mode="readonly") as mask_hdul:
        mask_hdul = ChipHDUList.fromfile(mask_path, mode="readonly")
        if mask_hdul[1].data.shape == self[1].data.shape:
            self.append(mask_hdul[1])
            self[-1].header["extname"] = "mask"
        else:
            raise ValueError("Mask shape does not match data shape")

    def get_data(self, masked=False):
        chip_img = self[1].data
        if masked:
            chip_mk_img = self.get_mask()
            chip_img = chip_img.astype(float)
            chip_img[chip_mk_img > 0] = np.nan
        return chip_img

    def set_data(self, new_data, idx=np.s_[:]):
        chip_img = self[1].data
        new_data = np.asarray(new_data)
        if new_data.shape == chip_img[idx].shape:
            chip_img[idx] = new_data
        else:
            raise ValueError("New data shape does not match index shape")

    def get_mask(self, copy=False):
        if (
            len(self) > 2
            and "mask" in [hdu.name for hdu in self]
            and "data" in [hdu.name for hdu in self]
        ):
            if copy:
                mk_img = self["mask"].data.copy()  # .astype(np.int16)
            else:
                mk_img = self["mask"].data
            return mk_img
        else:
            raise ValueError(
                "No mask available. Please use add_mask() to add a mask first."
            )

    def slice_cell_from_chip(self, cell, return_idx=False, masked=False):
        """
        slice a cell or a list of cells from a chip fits image

        Parameters
        ----------
        cell : str
            a cell name like "xy12" for a single cell or a set of spatially continuous cells like "xy[3:5][1:4]"
            in the latter case the gaps between the selected cells will be included
        return_idx : bool, optional
            If True, return the indices of pixels that belong to the selected cells, by default False
        masked : bool, optional
            If True then return masked data, by default False

        Returns
        -------
        numpy.ndarray
            2d array of the selected cell image
        """
        # assert chip_fits_pattern.search(cell)
        # assert cell in CELLS
        if cell in self.camera.cells:
            x1 = int(cell[2])
            x2 = x1 + 1
            y1 = int(cell[3])
            y2 = y1 + 1
        elif cell.startswith("xy"):
            cell_num_idx = eval("np.s_" + cell[2:])
            xs = np.arange(self.camera.num_cell_per_row)[cell_num_idx[0]]
            ys = np.arange(self.camera.num_cell_per_col)[cell_num_idx[1]]
            if np.array_equiv(xs, np.arange(xs.min(), xs.max() + 1)) and np.array_equiv(
                ys, np.arange(ys.min(), ys.max() + 1)
            ):
                x1, x2 = xs.min(), xs.max() + 1
                y1, y2 = ys.min(), ys.max() + 1
            else:
                raise ValueError("Cells must be spatially continuous.")
        else:
            raise ValueError("Invalid cell name.")
        chip_img = self.get_data(masked=masked)
        cell_idx = np.s_[
            y1
            * (self.camera.cell_num_pix_row + self.camera.cell_num_pix_row_gap) : y2
            * (self.camera.cell_num_pix_row + self.camera.cell_num_pix_row_gap)
            - self.camera.cell_num_pix_row_gap,
            x1
            * (self.camera.cell_num_pix_col + self.camera.cell_num_pix_col_gap) : x2
            * (self.camera.cell_num_pix_col + self.camera.cell_num_pix_col_gap)
            - self.camera.cell_num_pix_col_gap,
        ]
        cell_img = chip_img[cell_idx]
        if return_idx:
            return cell_img, cell_idx
        else:
            return cell_img

    def display(self, show_mask=False, ax=None, **kwargs):
        "Display the chip image with mask overlaid."
        if show_mask:
            mk_img = self.get_mask(copy=True)
            if show_mask is True:
                mk_img[mk_img < 1] = np.nan
            elif type(show_mask) is int:
                mk_img[mk_img != show_mask] = np.nan
                mk_img[mk_img == show_mask] = 1

        chip_img = self.get_data()
        if ax is None:
            fig, ax = plt.subplots(**kwargs)
        norm = ImageNormalize(chip_img, interval=ZScaleInterval())
        ax.imshow(chip_img, norm=norm, cmap="gray_r")
        if show_mask:
            ax.imshow(mk_img, cmap="autumn", alpha=0.6)
        ax.set_title(Path(self.filename()).name)


class CellHDUList(HDUList):
    def add_mask(self, mask_path):
        """
        append HDUList of the mask images to the data HDUList. Extremely slow because HDU can only be appended one by one.

        Args:
            mask_path (str or pathlib object): path to the mask fits file

        Raises:
            ValueError: _description_
            FileNotFoundError: _description_
        """
        if "mask" in [hdu.name for hdu in self]:
            raise ValueError("Mask already exists")
        mask_path = Path(mask_path)
        if not mask_path.is_file():
            # check if it is a nebulous path
            if neb_locate(mask_path):
                mask_path = neb_locate(mask_path)[0]["path"]
            else:
                raise FileNotFoundError(f"No such file: '{str(mask_path)}'")
        with read_cell(mask_path, trim_overscan=self.trim_overscan) as mask_hdul:
            assert len(self) == len(mask_hdul)
            for idx in range(1, len(self)):
                assert mask_hdul[idx].data.shape == self[idx].data.shape
                mask_hdul[idx].header["extname"] = (
                    mask_hdul[idx].header["extname"] + " mask"
                )
            self.extend(mask_hdul[1 : len(self)])

    def get_data(self, cell, masked=False):
        """
        return the image data of the cell

        Args:
            cell (str): cell name, e.g. from 'xy00' to 'xy77'
            masked (bool, optional): whether the masked pixels should be set to np.nan. Defaults to False.

        Returns:
            numpy.ndarray: 2d array of the cell image
        """
        assert cell in self.camera.cells
        cell_img = self[cell].data
        if masked:
            cell_mk_img = self.get_mask(cell)
            cell_img[cell_mk_img > 0] = np.nan
        return cell_img

    def get_kw_val(self, cell, kw):
        """
        return the value of a keyword kw in the header of the cell

        Args:
            cell (str): cell name, e.g. from 'xy00' to 'xy77'
            kw (str): keyword name

        Returns:
            str: keyword value
        """
        assert cell in self.camera.cells
        return self[cell].header.get(kw)

    def get_mask(self, cell):
        """
        return the mask data of the cell

        Args:
            cell (str): cell name, e.g. from 'xy00' to 'xy77'

        Raises:
            ValueError: when no mask is available

        Returns:
            numpy.ndarray: 2d array of the cell mask
        """
        assert cell in self.camera.cells
        if len(self) > self.camera.num_cell_per_chip + 1 and cell.upper() + " mask" in [
            hdu.name for hdu in self
        ]:
            mk_img = self[cell + " mask"].data
            return mk_img
        else:
            raise ValueError(
                "No mask available. Please use add_mask() to add a mask first."
            )

    def assemble_chip(
        self,
        trim_overscan=True,
        mask_data=False,
        mask_cells=None,
        subtract_bias=False,
        subtract_bkg=False,
    ):
        """
        assemble the cell images into a chip image
        """

        for y in range(8):
            for x in range(8):
                cell = f"xy{x}{y}"
                if cell in self.camera.cells:
                    cell_img = self.get_data(cell)
                else:
                    if self.trim_overscan:
                        cell_img = np.full(
                            (
                                self.camera.cell_num_pix_row,
                                self.camera.cell_num_pix_col,
                            ),
                            np.nan,
                        )
                    else:
                        cell_img = np.full(
                            (
                                self.camera.cell_num_pix_row_untrimmed,
                                self.camera.cell_num_pix_col_untrimmed,
                            ),
                            np.nan,
                        )
                if self.trim_overscan and cell_img.shape != (
                    self.camera.cell_num_pix_row,
                    self.camera.cell_num_pix_col,
                ):
                    cell_img = np.full(
                        (self.camera.cell_num_pix_row, self.camera.cell_num_pix_col),
                        np.nan,
                    )
                if not self.trim_overscan and cell_img.shape != (
                    self.camera.cell_num_pix_row_untrimmed,
                    self.camera.cell_num_pix_col_untrimmed,
                ):
                    cell_img = np.full(
                        (
                            self.camera.cell_num_pix_row_untrimmed,
                            self.camera.cell_num_pix_col_untrimmed,
                        ),
                        np.nan,
                    )
                if trim_overscan:
                    cell_img = cell_img[
                        : self.camera.cell_num_pix_row,
                        : self.camera.cell_num_pix_col,
                    ]
                if (
                    subtract_bias
                    and (bias_mean := self.get_kw_val(cell, "BIASLVL")) is not None
                ):
                    cell_img -= bias_mean
                if (
                    subtract_bkg
                    and (bkg_estimate := self.get_kw_val(cell, "BACKEST")) is not None
                ):
                    cell_img[
                        : self.camera.cell_num_pix_row,
                        : self.camera.cell_num_pix_col,
                    ] = (
                        cell_img[
                            : self.camera.cell_num_pix_row,
                            : self.camera.cell_num_pix_col,
                        ]
                        - bkg_estimate
                    )
                if mask_data:
                    cell_img[
                        : self.camera.cell_num_pix_row,
                        : self.camera.cell_num_pix_col,
                    ] = np.nan
                if mask_cells is not None:
                    if cell in mask_cells:
                        cell_img[:, :] = np.nan
                # reverse the cell pixels in the x direction
                cell_img = cell_img[:, ::-1]
                if x == 0:
                    row_img = cell_img
                else:
                    # print(row_img.shape, cell_img.shape)
                    row_img = np.hstack((row_img, cell_img))
            if y == 0:
                chip_img = row_img
            else:
                chip_img = np.vstack((chip_img, row_img))
        if trim_overscan:
            self.trim_overscan = True
        return chip_img
