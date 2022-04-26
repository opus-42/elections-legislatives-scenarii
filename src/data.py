"""Download source data."""
import logging
from pathlib import Path

import requests
import pandas as pd
from dagster import op, job
from sqlalchemy import REAL

from config import DATA_FOLDER, OPEN_DATA_HOST, RELOAD, DATA_SEP, ENCODING

logger = logging.getLogger(__name__)

DATASET_ID = "79b5cac4-4957-486b-bbda-322d80868224"
REPEATED_HEADERS = [
    "N. Panneau",
    "Sexe",
    "Nom",
    "Prénom",
    "Voix",
    "% Voix/Ins",
    "% Voix/Exp",
]


@op
def download_data() -> Path:

    url = f"{OPEN_DATA_HOST}/{DATASET_ID}"
    path = DATA_FOLDER / "data.txt"

    if path.exists():
        if RELOAD:
            path.unlink()
        else:
            return path

    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return path


@op
def compute_header(path: Path) -> tuple[list[str], int]:
    """Compute data header analysing length of lines."""

    # Get number of candidates
    with open(path, "r", encoding=ENCODING) as f:
        init_headers = f.readline()
        first_row = f.readline()

    nb_base_headers = len(init_headers.split(DATA_SEP)) - len(REPEATED_HEADERS)
    nb_candidates = (
        (len(first_row.split(DATA_SEP)) - nb_base_headers) /
        len(REPEATED_HEADERS)
    )

    assert nb_candidates.is_integer()
    nb_candidates = int(nb_candidates)

    # Create header list
    headers = init_headers.split(DATA_SEP)[:nb_base_headers]
    for i in range(nb_candidates):
        headers += [f"{f} Candidat {i+1}" for f in REPEATED_HEADERS]

    return headers, nb_candidates


@op
def load_data(path: Path, headers_config: tuple[list[str], int]) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        sep=DATA_SEP,
        encoding=ENCODING,
        low_memory=False,
        header=None,
        skiprows=[0]
    )

    # Configure columns
    headers, nb_candidates = headers_config
    df.columns = headers

    # Expanded
    dfs = []
    common_headers_length = df.shape[1] - len(REPEATED_HEADERS) * nb_candidates
    common_headers = headers[:common_headers_length]
    for i in range(nb_candidates):
        repeated_headers = [f"{f} Candidat {i+1}" for f in REPEATED_HEADERS]
        headers = common_headers + repeated_headers
        subset = df[headers].copy()
        subset.columns = common_headers + REPEATED_HEADERS
        dfs.append(subset)

    new_df = pd.concat(dfs)
    print(new_df)

    return df


@job
def get_data_job():
    load_data(
        path=download_data(),
        headers_config=compute_header(download_data())
    )
