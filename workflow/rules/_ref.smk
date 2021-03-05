rule get_mrc_via_opi:
    output:
        "results/dnb/MARC21/{isbn}.xml",
    params:
        access_token=get_dnb_access_token(),
    conda:
        "../envs/utils.yaml"
    log:
        "logs/dnb_download/{isbn}.log",
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
        "resources/bibliographic_data/{mrcfile}.mrc.gz",
    log:
        "logs/bibliographic_data/{mrcfile}.log",
    conda:
        "../envs/utils.yaml"
    shell:
        "curl -SL -o {output} https://data.dnb.de/DNB/{wildcards.mrcfile}.mrc.gz 2> {log}"


rule uncompress_mrc_gz:
    input:
        "resources/bibliographic_data/{mrcfile}.mrc.gz",
    output:
        "resources/bibliographic_data/{mrcfile}.mrc",
    log:
        "logs/unzip/{mrcfile}.log",
    conda:
        "../envs/utils.yaml"
    shell:
        "gzip -dkc {input} > {output}"
