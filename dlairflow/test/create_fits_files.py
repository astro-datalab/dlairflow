# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Create FITS files needed for testing.
"""
import sys
from importlib.resources import files
import numpy as np
from astropy.io import fits


def main():
    """Entry-point for command-line scripts.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    rng = np.random.default_rng()
    columns = [fits.Column(name='OBJ_ID', format='K',
                           array=rng.integers(low=2**32, high=2**63, size=10, dtype=np.int64)),
               fits.Column(name='RA', format='D',
                           array=360.0 * rng.random(10, dtype=np.float64)),
               fits.Column(name='DEC', format='D',
                           array=180.0 * rng.random(10, dtype=np.float64) - 90.0),
               fits.Column(name='Z', format='E',
                           array=rng.random(10, dtype=np.float32)),
               fits.Column(name='TARGET_BITS', format='J',
                           array=rng.integers(low=1, high=2**31, size=10, dtype=np.int32)),
               fits.Column(name='OBSERVED', format='L',
                           array=(rng.integers(low=1, high=100, size=10, dtype=np.int32) > 50)),
               fits.Column(name='OBJ_NAME', format='5A',
                           array=np.array(['a', 'bb', 'ccc', 'dddd', 'eeeee',
                                           'f', 'gg', 'hhh', 'iiii', 'jjjjj']))]
    #
    # Expected format.
    #
    # print(columns)
    hdulist = fits.HDUList([fits.PrimaryHDU(),
                            fits.BinTableHDU.from_columns(columns, name='FITS_TABLE')])
    hdulist.writeto(files('dlairflow.test') / 't' / 'test_validate_data_files.fits',
                    overwrite=True)
    #
    # Change the type of a column.
    #
    # print(columns)
    old_column = columns[3]
    columns[3] = fits.Column(name='Z', format='D',
                             array=rng.random(10, dtype=np.float64))
    hdulist = fits.HDUList([fits.PrimaryHDU(),
                            fits.BinTableHDU.from_columns(columns, name='FITS_TABLE')])
    hdulist.writeto(files('dlairflow.test') / 't' / 'test_validate_data_files_incompatible_type.fits',
                    overwrite=True)
    #
    # Reorder columns.
    #
    columns[3] = old_column
    rng.shuffle(columns)
    # print(columns)
    hdulist = fits.HDUList([fits.PrimaryHDU(),
                            fits.BinTableHDU.from_columns(columns, name='FITS_TABLE')])
    hdulist.writeto(files('dlairflow.test') / 't' / 'test_validate_data_files_reordered.fits',
                    overwrite=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
