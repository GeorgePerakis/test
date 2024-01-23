"""Tasks for managing the data."""

from pathlib import Path

import pandas as pd
import pytask

from Test.config import BLD, SRC
from Test.data_management import clean_data
from Test.utilities import read_yaml

clean_data_deps = {
    "scripts": Path("clean_data.py"),
    "data_info": SRC / "data_management" / "data_info.yaml",
    "data": SRC / "data" / "data.csv",
}


def task_clean_data_python(
    depends_on=clean_data_deps,
    produces=BLD / "python" / "data" / "data_clean.csv",
):
    """Clean the data (Python version)."""
    data_info = read_yaml(depends_on["data_info"])
    data = pd.read_csv(depends_on["data"])
    data = clean_data(data, data_info)
    data.to_csv(produces, index=False)


@pytask.mark.r(script=SRC / "data_management" / "clean_data.r", serializer="yaml")
@pytask.mark.depends_on(
    {
        "data_info": SRC / "data_management" / "data_info.yaml",
        "data": SRC / "data" / "data.csv",
    },
)
@pytask.mark.produces(BLD / "r" / "data" / "data_clean.csv")
def task_clean_data_r():
    """Clean the data (R version)."""
