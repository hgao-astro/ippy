[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## `ippy`

`ippy` is a Python package for interaction with Pan-STARRS Image Processing Pipeline (IPP) data products and databases and for helping perform operational tasks. It provides various functionalities to facilitate IPP-specific FITS IO and masks manipulation, and Nebulous tools to augment the existing `neb-*` command line tools of IPP. It was developed for internal use of the IPP development and operation team.

## Installation

Simple clone the repository and start using it:

```
git clone https://github.com/hgao-astro/ippy.git
cd ippy
pip install .
# or install a development copy
pip install -e .
```
Note that `ippy` is only tested with Python 3.6 and above.


## Usage

### FITS IO

IPP FITS images come in two formats: single chip image formed by 64 cells stitched together `XYxx.ch.[mk.]fits` and individual cell images in one multi-extension FITS file `XYxx.[mk.]fits`. `ippy.io` provides `ChipHDUList` and `CellHDUList` classes to facilitate the IO of the above two, respectively. Both are subclasses of `astropy.io.fits.HDUList` and are exposed through functions `read_chip` and `read_cell` as follows:

```python
>>> from ippy.io import read_chip, read_cell
>>> chip_hdul = read_chip("gpc1/OSS.nt/2024/01/04/o60313g0134o.2059532/o60313g0134o.2059532.ch.2704906.XY25.ch.fits", mask="gpc1/OSS.nt/2024/01/04/o60313g0134o.2059532/o60313g0134o.2059532.ch.2704906.XY25.ch.mk.fits")
>>> chip_hdul.info()
Filename: /data/ipp148.1/nebulous/d8/8f/17492822457.gpc1:OSS.nt:2024:01:04:o60313g0134o.2059532:o60313g0134o.2059532.ch.2704906.XY25.ch.fits
No.    Name      Ver    Type      Cards   Dimensions   Format
  0  PRIMARY       1 PrimaryHDU       6   ()
  1  data          1 CompImageHDU    427   (4846, 4868)   float32
  2  mask          1 CompImageHDU    464   (4846, 4868)   float32
>>> chip_img = chip_hdul.get_data(masked=True)
>>> cell_img = chip_hdul.slice_cell_from_chip("xy12", masked=True)
>>> chip_img.shape, cell_img.shape
((4868, 4846), (598, 590))
>>> chip_hdul.display(show_mask=True, dpi=200)
>>> plt.show()
```

<img src="docs/images/read_chip.png" height="300">

```python
>>> cell_hdul = read_cell("gpc1/20240104/o60313g0133o/o60313g0133o.ota23.fits")
>>> cell_hdul.info()
Filename: /data/ipp152.1/nebulous/1d/f4/17492785753.gpc1:20240104:o60313g0133o:o60313g0133o.ota23.fits
No.    Name      Ver    Type      Cards   Dimensions   Format
  0  PRIMARY       1 PrimaryHDU     397   ()
  1  xy00          1 CompImageHDU    142   (590, 598)   float32
  2  xy10          1 CompImageHDU    142   (590, 598)   float32
...  
 62  xy57          1 CompImageHDU    142   (590, 598)   float32
 63  xy67          1 CompImageHDU    142   (590, 598)   float32
 64  xy77          1 CompImageHDU    142   (590, 598)   float32
>>> cell_img = cell_hdul.get_data("xy24")
>>> chip_img = cell_hdul.assemble_chip()
>>> cell_img.shape, chip_img.shape
((598, 590), (4784, 4720))
```
Both subclasses supports IO with physical and Nebulous paths. Both enable optionally easy bundling of data image(s) and mask image(s) and applying masks by setting pixels to `NaN`. `ChipHDUList.display` displays the chip image in a linear and zscaled grey-scale figure, optionally with a mask overlaid. `ChipHDUList` also provides a method to slice out individual cells or a set of spatially continuous cells from the chip image. `CellHDUList` provides a method to assemble the cell images into a chip image (without gaps between the cells at the moment).

### Nightly Processing

### Nebulous Tools

### Misc.