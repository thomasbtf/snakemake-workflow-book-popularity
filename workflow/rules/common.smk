import pandas as pd


def get_isbns():
    return list(pep.sample_table["sample_name"].values)
