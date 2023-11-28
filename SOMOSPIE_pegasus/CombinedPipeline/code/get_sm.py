#!/usr/bin/env python3

import argparse
from osgeo import gdal # Install in a conda env: https://anaconda.org/conda-forge/gdal
import calendar
import subprocess
import shutil
from pathlib import Path
import numpy as np
import os
import concurrent.futures


def get_parser():
    parser = argparse.ArgumentParser(description='Arguments to fetch soil moisture data.')
    parser.add_argument('-y', "--year", type=int, choices=range(1970,2024), help='Year to process soil moisture data.')
    parser.add_argument('-m', "--month", type=int, choices=range(1,13), help='Month to process soil moisture data.')
    parser.add_argument('-o', "--output", help='Files with averages.')
    return parser

#Translate from namespaces to Python variables 
def from_args_to_vars (args):	
    year = int(args.year)
    month = int(args.month)
    output_file = args.output
    return year, month, output_file


def bash(argv):
    arg_seq = [str(arg) for arg in argv]
    proc = subprocess.Popen(arg_seq, stdout=subprocess.PIPE, stderr=subprocess.PIPE)#, shell=True)
    proc.wait() #... unless intentionally asynchronous
    stdout, stderr = proc.communicate()

    # Error catching: https://stackoverflow.com/questions/5826427/can-a-python-script-execute-a-function-inside-a-bash-script
    if proc.returncode != 0:
        raise RuntimeError("'%s' failed, error code: '%s', stdout: '%s', stderr: '%s'" % (
            ' '.join(arg_seq), proc.returncode, stdout.rstrip(), stderr.rstrip()))


def average_rasters(input_files, output_file):
    # All files must have the same extent
    command = ['gdal_calc.py']
    for f in input_files:
        command.append('-A')
        command.append(f)
    
    # command.append('--hideNoData')
    command.append('--overwrite')
    command.append('--format=GTiff')
    command.append('--outfile={}'.format(output_file))
    command.append('--calc="numpy.nanmean(A, axis=0)"')

    bash(command)

def merge_avg(input_files, output_file):
    vrt = gdal.BuildVRT('merged.vrt', input_files)
    vrt = None  # closes file

    with open('merged.vrt', 'r') as f:
        contents = f.read()

    if '<NoDataValue>' in contents:
        nodata_value = contents[contents.index('<NoDataValue>') + len('<NoDataValue>'): contents.index('</NoDataValue>')]# To add averaging function
    else:
        nodata_value = 0

    code = '''band="1" subClass="VRTDerivedRasterBand">
  <PixelFunctionType>average</PixelFunctionType>
  <PixelFunctionLanguage>Python</PixelFunctionLanguage>
  <PixelFunctionCode><![CDATA[
import numpy as np

def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize,raster_ysize, buf_radius, gt, **kwargs):
    data = np.ma.array(in_ar, mask=np.equal(in_ar, {}))
    np.ma.mean(data, axis=0, out=out_ar, dtype="float32")
    mask = np.all(data.mask,axis = 0)
    out_ar[mask] = {}
]]>
  </PixelFunctionCode>'''.format(nodata_value, nodata_value)

    sub1, sub2 = contents.split('band="1">', 1)
    contents = sub1 + code + sub2

    with open('merged.vrt', 'w') as f:
        f.write(contents)

    cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-co', 'TILED=YES', '-co', 'BIGTIFF=YES', '--config', 'GDAL_VRT_ENABLE_PYTHON', 'YES', 'merged.vrt', output_file]
    bash(cmd)
    os.remove('merged.vrt')


def reproject(input_file, output_file, projection):
    # Projection can be EPSG:4326, .... or the path to a wkt file
    warp_options = gdal.WarpOptions(dstSRS=projection, creationOptions=['COMPRESS=LZW', 'TILED=YES', 'BIGTIFF=YES', 'NUM_THREADS=ALL_CPUS'], multithread=True, warpOptions=['NUM_THREADS=ALL_CPUS'], dstNodata=np.nan, callback=gdal.TermProgress_nocb)
    warp = gdal.Warp(output_file, input_file, options=warp_options)
    warp = None  # Closes the files


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    year, month, output_file = from_args_to_vars(args)
    
    sm_files = ['NETCDF:./{0:04d}/{1:02d}_{2:02d}.nc:sm'.format(year, month, day) for day in range(1, calendar.monthrange(year, month)[1] + 1)]
    # average_rasters(sm_files, output_files[month - 1]) # Ignoring pixels with NaNs
    merge_avg(sm_files, output_file)

    # Change projection
    reproject(output_file, output_file, 'EPSG:4326')

    #shutil.rmtree('./{0:04d}'.format(year))
