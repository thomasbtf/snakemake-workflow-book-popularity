sys.stderr = open(snakemake.log[0], "w")

import altair as alt
import pandas as pd

no_records = 0
tag_list = []

# iterate over all input files
for tags_subfields in snakemake.input.tags:
    # open MARC record
    with open(tags_subfields, "r") as f:
        for line in f:
            tag_list.append(eval(line.strip()))

for no in snakemake.input.no_records:
    # open MARC record
    with open(no, "r") as f:
        for line in f:
            print(line)
            no_records += int(line.strip())


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

chart = chart.properties(title="Total records: {}".format(no_records))

chart.save(snakemake.output[0])
