import pandas as pd


def get_isbns():
    return list(pep.sample_table["sample_name"].values)


def get_dnb_access_token():
    with open(config["dnb_access_token"], "r") as access_token:
        return access_token.read()


def get_filenames(wildcards):
    with checkpoints.get_checksum.get().output[0].open() as f:
        df = pd.read_fwf(f)
    return list(
        df[df["Name der Datei"].str.endswith("mrc.gz")]["Name der Datei"].values
    )[0]


def get_checksum_sha(wildcards):
    with checkpoints.get_checksum.get().output[0].open() as f:
        df = pd.read_fwf(f)
    return df[df["Name der Datei"] == wildcards.file][
        "Pruefsumme der bereitgestellten Dateien gemaess dem Standard SHA256"
    ]
