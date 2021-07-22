#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
SDS data for MODIS
'''
import io
import yaml

sds = '''
SDS:
  MCD12Q1:
  - LC_Type1
  - LC_Type2
  - LC_Type3
  - LC_Type4
  - LC_Type5
  - LC_Prop1_Assessment
  - LC_Prop2_Assessment
  - LC_Prop3_Assessment
  - LC_Prop1
  - LC_Prop2
  - LC_Prop3
  - QC
  - LW
  MCD15A3H:
  - Fpar_500m
  - Lai_500m
  - FparLai_QC
  - FparExtra_QC
  - FparStdDev_500m
  - LaiStdDev_500m
  MOD10A1:
  - NDSI_Snow_Cover
  - NDSI_Snow_Cover_Basic_QA
  - NDSI_Snow_Cover_Algorithm_Flags_QA
  - NDSI
  - Snow_Albedo_Daily_Tile
  - orbit_pnt
  - granule_pnt
  MYD10A1:
  - NDSI_Snow_Cover
  - NDSI_Snow_Cover_Basic_QA
  - NDSI_Snow_Cover_Algorithm_Flags_QA
  - NDSI
  - Snow_Albedo_Daily_Tile
  - orbit_pnt
  - granule_pnt
'''

def get_sds(sds):
    """
    get_sds : return default SDS dictionary
    """
    with io.BytesIO(sds.encode('utf8')) as binary_file:
        with io.TextIOWrapper(binary_file, encoding='utf8') as file_obj:
            sds_info = yaml.load(file_obj,Loader=yaml.FullLoader)['SDS']
    return sds_info
