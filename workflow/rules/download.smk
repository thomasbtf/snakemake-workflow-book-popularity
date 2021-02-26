rule get_MARC21:
    output:
        "results/dnb/MARC21/{isbn}.xml",
    params:
        access_token = get_dnb_access_token()
    conda:
        "../envs/unix.yaml"
    log:
        "../logs/dnb_download/{isbn}.log"
    shell:
        "curl -sSL -o {output} 'http://services.dnb.de/sru/dnb?version=1.1&
        "operation=searchRetrieve&query=isbn%3D{wildcards.isbn}&recordSchema=
        "MARC21-xml&accessToken={params.access_token}' "
