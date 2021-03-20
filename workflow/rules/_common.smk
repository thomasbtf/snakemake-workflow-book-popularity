import pandas as pd
import csv

def get_isbns():
    return list(pep.sample_table["sample_name"].values)


def get_dnb_access_token():
    with open(config["dnb_access_token"], "r") as access_token:
        return access_token.read()


def get_filenames(wildcards):
    with checkpoints.get_checksum_for_mrc_gz.get().output[0].open() as f:
        df = pd.read_fwf(f)
    mrc_gz = list(
        df[df["Name der Datei"].str.endswith("mrc.gz")]["Name der Datei"].values
    )
    file_names = [entry.replace(".mrc.gz", "") for entry in mrc_gz]
    return file_names


def get_blurbs(wildcards):

    mrcfile_list = []
    isbn_list = []
    link_list = []

    mrcfiles = get_filenames(wildcards)

    for downloaded_mrcfile in mrcfiles:
        with checkpoints.extract_records_with_isbn.get(mrcfile=downloaded_mrcfile).output.uris.open() as f:
            
            csv_reader = csv.reader(f, delimiter="\t")
            for isbn, link in csv_reader:
                if "http://deposit.dnb.de/cgi-bin" in link:
                    mrcfile_list.append(downloaded_mrcfile)
                    isbn_list.append(isbn)
                    link_list.append(link)
    
    pattern = expand("data/blurbs/{mrcfile}/{isbn}~@~{link}.txt", zip, mrcfile=mrcfile_list, isbn=isbn_list, link=link_list)

    return pattern

