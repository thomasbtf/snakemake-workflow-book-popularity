sys.stderr = open(snakemake.log[0], "w")
threads = snakemake.threads

import altair as alt
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed


def parse_txt(tags_subfields_txt):
    print("Parsing", tags_subfields_txt)
    
    sub_dict = {}
    with open(tags_subfields_txt, "r") as f:
        for line in f:
            tags_subfield = eval(line.strip())
            if not tags_subfield in sub_dict.keys():
                sub_dict[tags_subfield] = 1
            else:
                sub_dict[tags_subfield] += 1

    print("Finished", tags_subfields_txt)
    return sub_dict

if __name__=="__main__":
    tag_dict = {}

    with ThreadPoolExecutor(max_workers=threads) as executor:
        sub_dicts = [executor.submit(parse_txt, path) for path in snakemake.input.tags]

        for sub_dict in as_completed(sub_dicts):
            sub = sub_dict.result()
            for key in sub:
                if key not in tag_dict.keys():
                    tag_dict[key] = sub[key]
                else:
                    tag_dict[key] += sub[key]
            del sub_dict, sub

    no_records = 0

    for no in snakemake.input.no_records:
        with open(no, "r") as f:
            for line in f:
                no_records += int(line.strip())

    tags_df = pd.DataFrame({"Tag-Subfield" : tag_dict.keys(), "Count": tag_dict.values()})
    tags_df['Tag'] = tags_df["Tag-Subfield"].apply(lambda x: x[0])
    
    print("No. of Tag-Subfield Combinations", len(tags_df))

    print('Plotting...')

    bars = (
        alt.Chart(tags_df)
        .mark_bar()
        .encode(
            x=alt.X("Count:Q", stack="zero"),
            y=alt.Y("Tag-Subfield:N"),
            color=alt.Color("Tag"),

        )
    )

    text = (
        alt.Chart(tags_df)
        .mark_text(dx=-7, dy=3, color="black")
        .encode(
            x=alt.X("Count:Q", stack="zero"),
            y=alt.Y("Tag-Subfield:N"),
            detail="Tag-Subfield:N",
            text=alt.Text("Count:N", format=".0f"),
            order=alt.Order("Tag-Subfield", sort="ascending"),
        )
    )

    chart = bars + text

    chart = chart.properties(title="Total records: {:,}".format(no_records))

    chart.save(snakemake.output[0])
