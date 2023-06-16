import numpy as np


def rms(x):
    """Return the root mean square of the elements of x."""
    x = np.asarray(x).ravel()  # ensure x is a 1d array
    return np.sqrt(np.nanmean(x**2))