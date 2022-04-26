"""Configuration file."""
from pathlib import Path

# Path
PROJECT_FOLDER = Path(__file__).parents[1]
DATA_FOLDER = PROJECT_FOLDER / "data"

# Source data config
OPEN_DATA_HOST = "https://www.data.gouv.fr/fr/datasets/r"
DATA_SEP = ";"
RELOAD = False
ENCODING = "ISO-8859-1"
