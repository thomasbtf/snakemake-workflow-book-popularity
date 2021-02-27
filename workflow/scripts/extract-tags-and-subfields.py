sys.stderr = open(snakemake.log[0], "w")

from pymarc import MARCReader

no_records = 0
tag_list = []

# iterate over all input files
for mrc in snakemake.input:
    # open MARC record
    with open(mrc, "rb") as fh:
        reader = MARCReader(fh)

        # iterate over all records
        for record in reader:
            no_records += 1

            # extract tag and subfield of records
            for tag in record.as_dict()["fields"]:
                for key, value in tag.items():
                    if type(value) is dict:
                        for subvalue in value["subfields"][0]:
                            tag_list.append((key, subvalue))
                    else:
                        tag_list.append((key, None))

with open(snakemake.output[0], "w") as f:
    for entry in tag_list:
        f.write(str(entry) +"\n")

with open(snakemake.output[1], "w") as f:
    f.write(str(no_records))