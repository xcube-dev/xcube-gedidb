# The MIT License (MIT)
# Copyright (c) 2025 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging

DATA_STORE_ID = "gedidb"

GEDIDB_URL = "https://s3.gfz.de"
GEDIDB_S3_BUCKET_NAME = "dog-ext--gedidb--gedi-l2-l4-v003"

NASA_CMR_URL = "https://cmr.earthdata.nasa.gov/search/collections.json?"

# Logging
LOG = logging.getLogger("xcube.gedidb")

# open params
GEDI_PRODUCT_LEVELS_DESCRIPTION = {
    "L2A": "Geolocated waveform data and relative height metrics.",
    "L2B": "Vegetation canopy cover and vertical profile metrics.",
    "L4A": "Aboveground biomass density estimates.",
    "L4C": "Gridded biomass estimates at global scales.",
}
# These concept IDs are required by the NASA CMR API to return the metadata
# for the requested processed level GEDI data
GEDI_PRODUCT_CONCEPT_IDS = {
    "L2A": "C3974616071-LPCLOUD",
    "L2B": "C3974616135-LPCLOUD",
    "L4A": "C4212593885-ORNL_CLOUD",
    "L4C": "C4302347663-ORNL_CLOUD",
}
SUPPORTED_VARIABLES = {
    "agbd": "L4A",
    "agbd_a1": "L4A",
    "agbd_a2": "L4A",
    "agbd_a5": "L4A",
    "agbd_pi_lower": "L4A",
    "agbd_pi_upper": "L4A",
    "agbd_se": "L4A",
    "agbd_t": "L4A",
    "agbd_t_se": "L4A",
    "beam_name": "L2A",
    "beam_type": "L2A",
    "cover": "L2B",
    "cover_z": "L2B",
    "cp_height": "L2B",
    "cp_pavd": "L2B",
    "degrade_flag": "L2A",
    "digital_elevation_model": "L2A",
    "digital_elevation_model_srtm": "L2A",
    "dz": "L2B",
    "elev_highestreturn_a1": "L2A",
    "elev_highestreturn_a2": "L2A",
    "elev_lowestmode": "L2A",
    "energy_total": "L2A",
    "fhd_normal": "L2B",
    "l2_algrunflag": "L2B",
    "l2a_quality_flag_rel2": "L2A",
    "l2a_quality_flag_rel3": "L2A",
    "l2b_quality_flag_rel2": "L2B",
    "l2b_quality_flag_rel3": "L2B",
    "l4a_quality_flag_rel3": "L4A",
    "l4c_quality_flag_rel3": "L4C",
    "landsat_treecover": "L2A",
    "landsat_water_persistence": "L2A",
    "leaf_off_flag": "L2A",
    "modis_nonvegetated": "L2A",
    "modis_nonvegetated_sd": "L2A",
    "modis_treecover": "L2A",
    "modis_treecover_sd": "L2A",
    "num_detectedmodes": "L2A",
    "omega": "L2B",
    "pai": "L2B",
    "pai_z": "L2B",
    "pavd_z": "L2B",
    "pft_class": "L2A",
    "pgap_theta": "L2B",
    "pgap_theta_error": "L2B",
    "phenology_phase": "L2A",
    "phenology_year": "L2A",
    "predict_stratum": "L4A",
    "predictor_limit_flag": "L4A",
    "rch": "L2B",
    "region_class": "L2A",
    "response_limit_flag": "L4A",
    "rg": "L2B",
    "rh": "L2A",
    "rhov_rhog": "L2B",
    "rhov_rhog_se": "L2B",
    "rossg": "L2B",
    "rv": "L2B",
    "rx_algrunflag": "L2A",
    "rx_maxamp": "L2A",
    "rx_range_highestreturn": "L2B",
    "sd_corrected": "L2A",
    "selected_algorithm": "L2A",
    "selected_l2a_algorithm": "L2B",
    "selected_rg_algorithm": "L2B",
    "sensitivity": "L2A",
    "sensitivity_a1": "L2A",
    "sensitivity_a2": "L2A",
    "shot_number": "L2A",
    "solar_azimuth": "L2A",
    "solar_elevation": "L2A",
    "stale_return_flag": "L2A",
    "surface_flag": "L2A",
    "toploc": "L2A",
    "urban_proportion": "L2A",
    "worldcover_class": "L2A",
    "wsci": "L4C",
    "wsci_pi_lower": "L4C",
    "wsci_pi_upper": "L4C",
    "wsci_xy": "L4C",
    "wsci_xy_pi_lower": "L4C",
    "wsci_xy_pi_upper": "L4C",
    "wsci_z": "L4C",
    "wsci_z_pi_lower": "L4C",
    "wsci_z_pi_upper": "L4C",
    "zcross": "L2A",
}
