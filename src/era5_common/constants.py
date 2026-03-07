"""Shared constants for ERA5 -> ILS conversion."""

from __future__ import annotations

FILL_VALUE_F32 = 1.0e20
SECONDS_PER_HOUR = 3600.0
WATER_DENSITY = 1000.0

RAW_PARAMETER_TO_VAR = {
    "2m_temperature": "t2m",
    "2m_dewpoint_temperature": "d2m",
    "surface_pressure": "sp",
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
    "surface_solar_radiation_downwards": "ssrd",
    "surface_thermal_radiation_downwards": "strd",
    "total_precipitation": "tp",
    "snowfall": "sf",
    "total_cloud_cover": "tcc",
}

VAR_TO_RAW_PARAMETER = {v: k for k, v in RAW_PARAMETER_TO_VAR.items()}

TARGET_TO_INPUT_PARAMETERS = {
    "Tair": ["2m_temperature"],
    "Qair": ["2m_dewpoint_temperature", "surface_pressure"],
    "PSurf": ["surface_pressure"],
    "Wind": ["10m_u_component_of_wind", "10m_v_component_of_wind"],
    "SWdown": ["surface_solar_radiation_downwards"],
    "LWdown": ["surface_thermal_radiation_downwards"],
    "Precip": ["total_precipitation"],
    "Rainf": ["total_precipitation", "snowfall"],
    "Snowf": ["snowfall"],
    "CCover": ["total_cloud_cover"],
}

TARGET_VARS = tuple(TARGET_TO_INPUT_PARAMETERS.keys())

TARGET_METADATA = {
    "Tair": {
        "standard_name": "air_temperature",
        "long_name": "Near surface air temperature",
        "units": "K",
        "alma_name": "Tair",
        "amip_name": "tas",
    },
    "Qair": {
        "standard_name": "specific_humidity",
        "long_name": "Near surface specific humidity",
        "units": "kg kg-1",
        "alma_name": "Qair",
        "amip_name": "huss",
    },
    "PSurf": {
        "standard_name": "surface_air_pressure",
        "long_name": "Surface Pressure",
        "units": "Pa",
        "alma_name": "PSurf",
        "amip_name": "ps",
    },
    "Wind": {
        "standard_name": "wind_speed",
        "long_name": "Near surface wind speed",
        "units": "m s-1",
        "alma_name": "Wind",
        "amip_name": "sfcWind",
    },
    "SWdown": {
        "standard_name": "surface_downwelling_shortwave_flux_in_air",
        "long_name": "Downward Shortwave Radiation",
        "units": "W m-2",
        "alma_name": "SWdown",
        "amip_name": "rsds",
    },
    "LWdown": {
        "standard_name": "surface_downwelling_longwave_flux_in_air",
        "long_name": "Downward Longwave Radiation",
        "units": "W m-2",
        "alma_name": "LWdown",
        "amip_name": "rlds",
    },
    "Precip": {
        "standard_name": "rainfall_flux",
        "long_name": "Rainfall rate",
        "units": "kg m-2 s-1",
        "alma_name": "Rainf",
        "amip_name": "prra",
    },
    "Rainf": {
        "standard_name": "rainfall_flux",
        "long_name": "Rainfall rate",
        "units": "kg m-2 s-1",
        "alma_name": "Rainf",
        "amip_name": "prra",
    },
    "Snowf": {
        "standard_name": "snowfall_flux",
        "long_name": "Snowfall rate",
        "units": "kg m-2 s-1",
        "alma_name": "Snowf",
        "amip_name": "prsn",
    },
    "CCover": {
        "standard_name": "cloud_area_fraction",
        "long_name": "Total cloud fraction",
        "units": "-",
        "alma_name": "CCover",
        "amip_name": "clt",
    },
}

STAGE1_UNITS = {
    "Tair": "K",
    "Qair": "kg kg-1",
    "PSurf": "Pa",
    "Wind": "m s-1",
    "SWdown": "J m-2",
    "LWdown": "J m-2",
    "Precip": "m",
    "Rainf": "m",
    "Snowf": "m of water equivalent",
    "CCover": "-",
}

TIME_OUTPUT_UNITS = "hours since 1871-01-01 00:00:00"
TIME_OUTPUT_CALENDAR = "proleptic_gregorian"

DEFAULT_ATTRS = {
    "Conventions": "CF-1.6",
    "conventions": "ALMA-3, CF-1.6, AMIP",
    "institution": "Institute of Industrial Science, The University of Tokyo",
    "title": "Global Soil Wetness Project 3 <http://hydro.iis.u-tokyo.ac.jp/GSWP3> EXP1 forcing data",
    "creator": "Hyungjun Kim <hjkim@iis.u-tokyo.ac.jp>",
    "contributors": (
        "Satoshi Watanabe, Eun-Chul Chang, Nobuyuki Utsumi, Kei Yoshimura, "
        "Gilbert Compo, Hirabayashi Yukiko, James Famiglietti, and Taikan Oki"
    ),
}
