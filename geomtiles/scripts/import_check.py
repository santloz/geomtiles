"""Script de comprobación rápida de imports locales.

English:
Quick script to verify that key package modules import correctly when the
package root is added to sys.path. Useful during development.
"""

import importlib
import os
import sys

# Ensure package root (geomtiles/) is on sys.path for local imports
# English: ensure package root is available for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

try:
    importlib.import_module("geo_tiles.utils.tile_cache")
    importlib.import_module("geo_tiles.repositories.metadata")
    importlib.import_module("geo_tiles.services")
    print("IMPORT_OK")
except Exception as e:
    print("IMPORT_ERROR", repr(e))
    sys.exit(1)
