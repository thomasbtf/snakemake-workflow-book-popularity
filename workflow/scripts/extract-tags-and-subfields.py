from multiprocessing import cpu_count, Manager
from joblib import Parallel, delayed
from tqdm import tqdm 

from pymarc import MARCReader


class Counter(object):
    def __init__(self, manager, initval=0):
        self.val = manager.Value("i", initval)
        self.lock = manager.Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value


def get_tags(record, no_records):

    no_records.increment()

    # extract tag and subfield of records
    for tag in record.as_dict()["fields"]:
        for key, value in tag.items():
            if type(value) is dict:
                for subvalue in value["subfields"][0]:
                    tag_list.append((key, subvalue))
            else:
                tag_list.append((key, None))


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    sm_input = snakemake.input
    sm_output = snakemake.output
    num_cores = snakemake.threads

    # sm_input = ["resources/bibliographic_data/dnb_all_dnbmarc_20201013-1.mrc"]
    # sm_output = ["results/analysis/raw-data/tags-and-subfields/dnb_all_dnbmarc_20201013-1.txt", "results/analysis/raw-data/tags-and-subfields/records_in_dnb_all_dnbmarc_20201013-1.txt"]
    # num_cores = cpu_count()

    with Manager() as manager:
        no_records = Counter(manager, 0)
        tag_list = manager.list()

        with open(sm_input[0], "rb") as marc_file_handle:
            Parallel(n_jobs=num_cores)(
                delayed(get_tags)(record, no_records)
                for record in tqdm(MARCReader(marc_file_handle))
            )

        with open(sm_output[0], "w") as f:
            for entry in tag_list:
                f.write(str(entry) + "\n")

        with open(sm_output[1], "w") as f:
            f.write(str(no_records.value()))
