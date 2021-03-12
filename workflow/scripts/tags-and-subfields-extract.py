sys.stderr = open(snakemake.log[0], "w")

from pymarc import MARCReader, record
from tqdm import tqdm

import itertools


def single_loop(sm_input, sm_output):

    no_records = 0

    with open(sm_input[0], "rb") as marc_file_handle:
        reader = tqdm(MARCReader(marc_file_handle), mininterval=30)

        with open(sm_output[0], "w") as output:
            for record in reader:
                no_records += 1
                for tag in record.as_dict()["fields"]:
                    for key, value in tag.items():
                        if type(value) is dict:
                            for subvalue in value["subfields"][0]:
                                output.write(str((key, subvalue)) + "\n")
                        else:
                            output.write(str((key, None)) + "\n")

    with open(sm_output[1], "w") as output:
        output.write(str(no_records) + "\n")


if __name__ == "__main__":
    sm_input = snakemake.input
    sm_output = snakemake.output
    # sm_input = ["resources/bibliographic_data/dnb_all_dnbmarc_20210213-1.mrc"]
    # sm_output = ["results/analysis/tags-subfields-combos/dnb_all_dnbmarc_20201013-1-XXX.txt", "results/analysis/tags-subfields-combos/records_in_dnb_all_dnbmarc_20201013-1-XXX.txt"]

    single_loop(sm_input, sm_output)
