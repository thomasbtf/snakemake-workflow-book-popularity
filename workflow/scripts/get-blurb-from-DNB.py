from bs4 import BeautifulSoup

import requests

def download_and_save_blurb(link, save_path):
    try:
        r = requests.get(link)
        assert r.status_code == 200
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    soup = BeautifulSoup(r.content, "lxml")

    with open(save_path, "w") as soup_writer:
        soup_writer.write(soup.p.get_text())


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
  
    download_and_save_blurb(
        snakemake.wildcards.link,
        snakemake.output[0],
    )
