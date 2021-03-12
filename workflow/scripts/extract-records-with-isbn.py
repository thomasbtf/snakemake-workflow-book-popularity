sys.stderr = open(snakemake.log[0], "w")

import xml.etree.ElementTree as ET

from pymarc import MARCReader, record_to_xml_node
from tqdm import tqdm


def extract_records_with_isbn(in_mrc, out_csv, out_uris):

    # iterator for dnb marc file
    with open(in_mrc, "rb") as marc_file_handle, open(
        out_csv, "wb"
    ) as csv_handle, open(out_uris, "w") as uri_handle:
        reader = tqdm(MARCReader(marc_file_handle), mininterval=30)

        # iterate over records in iterator
        for record in reader:
            # check if records has isbn
            if record.isbn():
                # save the record in xml
                csv_handle.write(
                    ET.tostring(record_to_xml_node(record), encoding="utf-8")
                    + "\n".encode("utf-8")
                )

                # extract links to parse from record
                for link_tag in record.get_fields("856"):
                    for link in link_tag.get_subfields("u"):
                        uri_handle.write(f"{record.isbn()}\t{link}\n")


if __name__ == "__main__":
    path_to_mrc_file = snakemake.input[0]
    path_to_mrc_xml = snakemake.output.xmls
    path_to_uri_csv = snakemake.output.uris

    extract_records_with_isbn(path_to_mrc_file, path_to_mrc_xml, path_to_uri_csv)
