rule uncompress_mrc_gz:
    input:
        "{prefix}.mrc.gz",
    output:
        "{prefix}.mrc",
    log:
        "logs/uncompress/{prefix}.log",
    conda:
        "../envs/utils.yaml"
    shell:
        "gzip -dkc {input} > {output}"
