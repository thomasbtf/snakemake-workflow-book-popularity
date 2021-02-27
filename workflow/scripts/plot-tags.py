sys.stderr = open(snakemake.log[0], "w")

import altair as alt
import pandas as pd

from pymarc import MARCReader

no_of_records = 0
tag_list = []

# iterate over all input files
for mrc in snakemake.input:
    # open MARC record
    with open(mrc, "rb") as fh:
        reader = MARCReader(fh)

        # iterate over all records
        for record in reader:
            no_of_records += 1

            # extract tag and subfield of records
            for tag in record.as_dict()["fields"]:
                for key, value in tag.items():
                    if type(value) is dict:
                        for subvalue in value["subfields"][0]:
                            tag_list.append((key, subvalue))
                    else:
                        tag_list.append((key, None))

tags_df = pd.DataFrame(tag_list, columns=["Tag", "Subfield"])

bars = (
    alt.Chart(tags_df)
    .mark_bar()
    .encode(
        x=alt.X("count(Tag):N", stack="zero"),
        y=alt.Y("Tag:N"),
        color=alt.Color("Subfield"),
        order=alt.Order("Subfield", sort="ascending"),
    )
)

text = (
    alt.Chart(tags_df)
    .mark_text(dx=-15, dy=3, color="white")
    .encode(
        x=alt.X("count(Tag):N", stack="zero"),
        y=alt.Y("Tag:N"),
        detail="Subfield:N",
        text=alt.Text("count(Tag):N", format=".0f"),
        order=alt.Order("Subfield", sort="ascending"),
    )
)

chart = bars + text

chart = chart.properties(title="Total records: {}".format(no_of_records))

chart.save(snakemake.output[0])
