rule extract_tags_and_subfields:
    input:
        "resources/bibliographic_data/{mrcfile}.mrc",
    output:
        protected("results/analysis/tags-subfields-combos/{mrcfile}.txt"),
        protected("results/analysis/tags-subfields-combos/records_in_{mrcfile}.txt"),
    log:
        "logs/tags-subfields-combos/{mrcfile}.log",
    conda:
        "../envs/pymarc.yaml"
    script:
        "../scripts/extract-tags-and-subfields.py"


rule plot_tags:
    input:
        tags=lambda wildcards: expand(
            "results/analysis/tags-subfields-combos/{mrcfile}.txt",
            file=get_filenames(wildcards),
        ),
        no_records=lambda wildcards: expand(
            "results/analysis/tags-subfields-combos/records_in_{mrcfile}.txt",
            file=get_filenames(wildcards),
        ),
    output:
        "results/plots/tag-overview.svg",
    log:
        "logs/plots/tag-overview.log",
    conda:
        "../envs/plotting.yaml"
    threads: 4
    script:
        "../scripts/plot-tags.py"
