from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Queue

from pymarc import MARCReader, record
from tqdm import tqdm

import itertools
from timeit import timeit


def get_tags_and_subfields(record):
    sublist = []
    for tag in record.as_dict()["fields"]:
        for key, value in tag.items():
            if type(value) is dict:
                for subvalue in value["subfields"][0]:
                    sublist.append(str((key, subvalue)))
            else:
                sublist.append(str((key, None)))
    return sublist


def thread_map():
    sm_input = ["resources/bibliographic_data/dnb_all_dnbmarc_20201013-1.mrc"]
    sm_output = ["results/analysis/tags-subfields-combos/dnb_all_dnbmarc_20201013-1-XXX.txt", "results/analysis/tags-subfields-combos/records_in_dnb_all_dnbmarc_20201013-1-XXX.txt"]

    no_records=0

    with open(sm_input[0], "rb") as marc_file_handle:
        reader = itertools.islice(MARCReader(marc_file_handle), 1000)

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = executor.map(get_tags_and_subfields, reader)

            with open(sm_output[0], "w") as f:
                for results in results:
                    no_records +=1
                    f.writelines(results)

    with open(sm_output[1], "w") as f:
        f.write(str(no_records))


def thread_submit(sm_input, sm_output):
    # sm_input = ["resources/bibliographic_data/dnb_all_dnbmarc_20201013-1.mrc"]
    # sm_output = ["results/analysis/tags-subfields-combos/dnb_all_dnbmarc_20201013-1-XXX.txt", "results/analysis/tags-subfields-combos/records_in_dnb_all_dnbmarc_20201013-1-XXX.txt"]

    no_records=0

    with open(sm_input[0], "rb") as marc_file_handle:
        reader = MARCReader(marc_file_handle)

        with ThreadPoolExecutor(max_workers=8) as executor:
            submits = [executor.submit(get_tags_and_subfields, record) for record in reader]

            with open(sm_output[0], "w") as f:
                for submit in as_completed(submits):
                    no_records +=1
                    f.writelines(submit.result())

    with open(sm_output[1], "w") as f:
        f.write(str(no_records))


def get_tags_and_subfields_loop():
    sm_input = ["resources/bibliographic_data/dnb_all_dnbmarc_20201013-1.mrc"]
    sm_output = ["results/analysis/tags-subfields-combos/dnb_all_dnbmarc_20201013-1-XXX.txt", "results/analysis/tags-subfields-combos/records_in_dnb_all_dnbmarc_20201013-1-XXX.txt"]

    tag_list = []
    no_records = 0

    with open(sm_input[0], "rb") as marc_file_handle:
        reader = itertools.islice(MARCReader(marc_file_handle), 1000)
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


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    sm_input = snakemake.input
    sm_output = snakemake.output

    thread_submit(sm_input, sm_output)
