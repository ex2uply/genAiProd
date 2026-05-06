"""Load banking transactions."""
import logging
from pathlib import Path

import pandas as pd

LOG = logging.getLogger(__name__)


def load_data(file_path: Path) -> pd.DataFrame:
    """Load transaction data from CSV."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        LOG.error("Error loading file: %s", e)
        raise
