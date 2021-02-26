import pandas as pd


def get_isbns():
    return list(pep.sample_table["sample_name"].values)


def get_dnb_access_token():
    with open(config["dnb_access_token"], "r") as access_token:
        return access_token.read()
