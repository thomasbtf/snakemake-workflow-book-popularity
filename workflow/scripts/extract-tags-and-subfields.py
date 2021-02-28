from pymarc import MARCReader
from tqdm import tqdm


def get_tags_and_subfields(sm_input, sm_output):

    tag_list = []
    no_records = 0

    with open(sm_input[0], "rb") as marc_file_handle:
        with tqdm(MARCReader(marc_file_handle)) as reader:
            for record in reader:
                no_records += 1
                for tag in record.as_dict()["fields"]:
                    for key, value in tag.items():
                        if type(value) is dict:
                            for subvalue in value["subfields"][0]:
                                tag_list.append((key, subvalue))
                        else:
                            tag_list.append((key, None))

    str_list = [str(entry) + "\n" for entry in tag_list]

    with open(sm_output[0], "w") as f:
        f.writelines(str_list)

    with open(sm_output[1], "w") as f:
        f.write(str(no_records))


if __name__ == "__main__":
    # sys.stderr = open(snakemake.log[0], "w")
    sm_input = snakemake.input
    sm_output = snakemake.output

    # sm_input = ["resources/bibliographic_data/dnb_all_dnbmarc_20201013-1.mrc"]
    # sm_output = ["results/analysis/raw-data/tags-and-subfields/dnb_all_dnbmarc_20201013-1.txt", "results/analysis/raw-data/tags-and-subfields/records_in_dnb_all_dnbmarc_20201013-1.txt"]

    get_tags_and_subfields(sm_input, sm_output)
