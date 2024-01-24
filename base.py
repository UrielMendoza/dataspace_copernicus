# General definitions and configuration
# Set the Copernicus access credentials here

# ------------------------------------------------------------------------------

# Copernicus API access credentials
# Insert your api credencials
# From DataSpace
Dataspace_username = ''
Dataspace_password = ''

# ------------------------------------------------------------------------------

# Some tiles of interest in UTM/MGRS format

# SENTINEL-2A and SENTINEL-2B occupy the same orbit, but separated by 180 degrees. The mean orbital altitude is 786 km. The orbit inclination is 98.62 and the Mean Local Solar Time (MLST) at the descending node is 10:30.

tiles = {}

# Test tiles
tiles["test"] = [
"16QEJ", "16QDH"
]
# ------------------------------------------------------------------------------
