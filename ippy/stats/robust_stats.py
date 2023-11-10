# from math import erf

import numpy as np
from astropy.stats import sigma_clip

# from numpy.typing import ArrayLike


def sigma_clip_stats(
    x, sigma=3, sigma_lower=None, sigma_upper=None, axis=None, maxiters=10, ngood_min=1
):
    """
    wrapper of astropy.stats.sigma_clip

    Returns
    -------
    _type_
        _description_
    """
    # x = np.asarray(x).ravel()
    clipped = sigma_clip(
        x,
        sigma=sigma,
        sigma_lower=sigma_lower,
        sigma_upper=sigma_upper,
        axis=axis,
        maxiters=maxiters,
        masked=False,
    )
    ngood = np.isfinite(clipped).sum(axis=axis)
    mean = np.nanmean(clipped, axis=axis)
    median = np.nanmedian(clipped, axis=axis)
    std = np.nanstd(clipped, axis=axis)
    mean_err = std / np.sqrt(ngood)
    if ngood.ndim < 1:
        if ngood < ngood_min:
            return np.nan, np.nan, np.nan, np.nan
        else:
            return mean, median, std, mean_err
    idx = ngood < ngood_min
    mean[idx] = np.nan
    median[idx] = np.nan
    std[idx] = np.nan
    mean_err[idx] = np.nan
    return mean, median, std, mean_err


def robust_binned_stats(
    x, y, *, statistic="mean", bins=10, nsigma=3, maxiters=10, ngood_min=1
):
    x = np.asarray(x).ravel()
    idx_sort = np.argsort(x)
    x = x[idx_sort]
    if not isinstance(y, list):
        y = np.asarray(y).ravel()
        if y.shape != x.shape:
            raise ValueError("x and y must have the same shape")
        y = [y[idx_sort]]
    else:
        for i in range(len(y)):
            y[i] = np.asarray(y[i]).ravel()
            if y[i].shape != x.shape:
                raise ValueError("x and each elemeent of y must have the same shape")
            y[i] = y[i][idx_sort]
    if isinstance(bins, int):
        if bins < 1:
            raise ValueError(
                "bins must be a positive integer when specifying the number of bins"
            )
        nbins = bins
        bins = np.linspace(x.min(), x.max(), nbins + 1)
    else:
        try:
            nbins = len(bins) - 1
        except TypeError:
            raise TypeError("bins must be an integer or a sequence")
        if nbins < 1:
            raise ValueError(
                "bins must encompass at least one bin when specifying the edges of bins"
            )
    bins_num = np.digitize(x, bins)
    # include the last point in the last bin, don't want to have a single bin that contains only the last point
    bins_num[-1] = nbins
    res = []
    for i in range(len(y)):
        y_ = y[i]
        res_ = np.full(nbins, np.nan)
        for i in range(nbins):
            idx_bin = bins_num == i + 1
            mean, median, std, mean_err = sigma_clip_stats(
                y_[idx_bin], sigma=nsigma, maxiters=maxiters, ngood_min=ngood_min
            )
            if statistic == "mean":
                res_[i] = mean
            if statistic == "median":
                res_[i] = median
            if statistic == "std":
                res_[i] = std
            if statistic == "mean err":
                res_[i] = mean_err
        res.append(res_)
    if len(res) == 1:
        return res[0]
    else:
        return res
