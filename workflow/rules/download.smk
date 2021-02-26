rule get_MARC21:
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


checkpoint get_checksum:
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
        "resources/bibliographic_data/{file}",
    log:
        "../logs/bibliographic_data/{file}.log",
    conda:
        "../envs/utils.yaml"
    shell:
        "curl -SL -o {output} https://data.dnb.de/DNB/{wildcards.file} 2> {log}"


rule download:
    input:
        lambda wildcards: expand(
            "resources/bibliographic_data/{file}", file=get_filenames(wildcards)
        ),
    output:
        touch("resources/bibliographic_data/data.downloaded"),
