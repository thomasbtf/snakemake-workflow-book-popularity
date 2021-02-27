rule get_mrc_via_opi:
    output:
        "results/dnb/MARC21/{isbn}.xml",
    params:
        access_token=get_dnb_access_token(),
    conda:
        "../envs/utils.yaml"
    log:
        "../logs/dnb_download/{isbn}.log",
    shell:
        "curl -sSL -o {output} 'http://services.dnb.de/sru/dnb?version=1.1&"
        "operation=searchRetrieve&query=isbn%3D{wildcards.isbn}&recordSchema="
        "MARC21-xml&accessToken={params.access_token}' "


checkpoint get_checksum_for_mrc_gz:
    output:
        temp("resources/bibliographic_data/checksum.txt"),
    log:
        "logs/bibliographic_data/get-checksum.log",
    conda:
        "../envs/utils.yaml"
    shell:
        "curl -SL -o {output} https://data.dnb.de/DNB/001_Pruefsumme_Checksum.txt 2> {log}"


rule get_bibliographic_data:
    output:
        "resources/bibliographic_data/{file}.mrc.gz",
    log:
        "../logs/bibliographic_data/{file}.log",
    conda:
        "../envs/utils.yaml"
    shell:
        "curl -SL -o {output} https://data.dnb.de/DNB/{wildcards.file}.mrc.gz 2> {log}"


rule uncompress_mrc_gz:
    input:
        "resources/bibliographic_data/{file}.mrc.gz"
    output:
        "resources/bibliographic_data/{file}.mrc",
    log:
        "../logs/unzip/{file}.log"
    conda:
        "../envs/utils.yaml"
    shell:
        "gzip -dkc {input} > {output}"


rule parse_mrc:
    input:
        "resources/bibliographic_data/dnb_all_dnbmarc_20201013-1.mrc",
    log:
        "../logs/pymarc.log"
    conda:
        "../envs/pymarc.yaml"
    script:
        "../scripts/parse-pymarc.py"

rule plot_tags:
    input:
        lambda wildcards: expand("resources/bibliographic_data/{file}.mrc", file = get_filenames(wildcards)),
    output:
        "results/plots/tag-overview.svg",
    log:
        "../logs/plots/tag-overview.log"
    conda:
        "../envs/plotting.yaml"
    script:
        "../scripts/plot-tags.py"