rule extract_books_with_isbn:
    input:
        "",
    output:
        "",
    log:
        "logs/extract-books-with-isbn.log",
    conda:
        "../envs/pymarc.yaml"
    script:
        "../scripts/extract-books-with-isbn.py"
