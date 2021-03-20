
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
        "resources/bibliographic_data/{mrcfile}.mrc.gz",
    log:
        "logs/bibliographic_data/{mrcfile}.log",
    conda:
        "../envs/utils.yaml"
    shell:
        "curl -SL -o {output} https://data.dnb.de/DNB/{wildcards.mrcfile}.mrc.gz 2> {log}"


checkpoint extract_records_with_isbn:
    input:
        "resources/bibliographic_data/{mrcfile}.mrc",
    output:
        xmls="data/raw-records/isbn-{mrcfile}.xml",
        uris="data/URIs/_to-parse-from-{mrcfile}.tsv",
    log:
        "logs/extract-records-with-isbn/{mrcfile}.log",
    conda:
        "../envs/pymarc.yaml"
    script:
        "../scripts/extract-records-with-isbn.py"


rule get_blurb_from_DNB:
    output:
        "data/blurbs/{mrcfile}/{isbn}~@~{link}.txt",
    log:
        "logs/get-blurb-from-DNB/{mrcfile}/{isbn}~@~{link}.log",
    conda:
        "../envs/beautifulsoup.yaml"
    script:
        "../scripts/get-blurb-from-DNB.py"
