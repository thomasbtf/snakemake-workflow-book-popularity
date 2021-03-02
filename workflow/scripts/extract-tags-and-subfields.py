sys.stderr = open(snakemake.log[0], "w")


from pymarc import MARCReader, record
from tqdm import tqdm


def single_loop(sm_input, sm_output):
    # sm_input = ["resources/bibliographic_data/dnb_all_dnbmarc_20201013-1.mrc"]
    # sm_output = ["results/analysis/tags-subfields-combos/dnb_all_dnbmarc_20201013-1-XXX.txt", "results/analysis/tags-subfields-combos/records_in_dnb_all_dnbmarc_20201013-1-XXX.txt"]

    no_records = 0

    with open(sm_input[0], "rb") as marc_file_handle:
        reader = tqdm(MARCReader(marc_file_handle))

        with open(sm_output[0], "w") as output:
            for record in reader:
                no_records += 1
                for tag in record.as_dict()["fields"]:
                    for key, value in tag.items():
                        if type(value) is dict:
                            for subvalue in value["subfields"][0]:
                                output.writelines(str((key, subvalue)))
                        else:
                            output.writelines(str((key, None)))
    
    with open(sm_output[1], "w") as output:
        output.write(str(no_records))

if __name__ == "__main__":
    sm_input = snakemake.input
    sm_output = snakemake.output

    single_loop(sm_input, sm_output)
