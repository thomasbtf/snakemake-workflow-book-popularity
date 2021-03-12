rule extract_records_with_isbn:
    input:
        "resources/bibliographic_data/{mrcfile}.mrc",
    output:
        xmls="data/raw-records/isbn-{mrcfile}.xml",
        uris="data/URIs/_to-parse-from-{mrcfile}.tsv"
    log:
        "logs/extract-records-with-isbn/{mrcfile}.log",
    conda:
        "../envs/pymarc.yaml"
    script:
        "../scripts/extract-records-with-isbn.py"


rule get_blurbs:
    input:
        "data/URIs/_to-parse-from-{mrcfile}.tsv",
    output:
        "data/blurbs/_parsed-blurbs-from-{mrcfile}.tsv",
    params:
        outdir=lambda w, output: os.path.dirname(output[0]),
    threads: 4
    log:
        "logs/get-blurbs/{mrcfile}.log"
    conda:
        "../envs/beautifulsoup.yaml"
    script:
        "../scripts/get-blurbs.py"