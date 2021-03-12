from concurrent.futures import ThreadPoolExecutor, as_completed, thread
from threading import BoundedSemaphore
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm

import requests
import csv
import time


def download_and_save_blurb(isbn, link, outdir):
    try:
        r = requests.get(link)
        assert r.status_code == 200
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    soup = BeautifulSoup(r.content, "lxml")

    outpath = f"{outdir}/{isbn}.txt"
    with open(outpath, "w") as soup_writer:
        soup_writer.write(soup.p.get_text())

    return (isbn, outpath)


def get_blurbs_to_download(path_to_link_csv, path_to_summary_file, outdir, max_threads):

    # open csv file with links
    print("Opening context managers", file=sys.stderr)
    with open(path_to_link_csv, "r") as csv_file:

        # create parser for csv file
        print("Creating csv reader.", file=sys.stderr)
        csv_reader = csv.reader(csv_file, delimiter="\t")

        # start threads
        print("Start submitting jobs.", file=sys.stderr)
        futures = []
        with ThreadPoolExecutor(max_workers=max_threads) as executor, open(
            path_to_summary_file, "w"
        ) as summary_writer:
            for isbn, link in tqdm(csv_reader, mininterval=1):
                if "http://deposit.dnb.de/cgi-bin" in link:
                    futures.append(
                        executor.submit(download_and_save_blurb, isbn, link, outdir)
                    )

            # save results as they are completed to the summary file
            print("Finished submitting jobs. Waiting for results.", file=sys.stderr)

            with tqdm(total=len(futures), mininterval=30) as pbar:
                for cmpl_submit in as_completed(futures):

                    if cmpl_submit.exception() is not None:
                        raise cmpl_submit.exception()

                    isbn, save_path = cmpl_submit.result()
                    summary_writer.write(f"{isbn}\t{save_path}\n")
                    pbar.update(1)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    max_threads = snakemake.threads

    if max_threads < 1:
        max_threads = 1

    max_threads += 4

    t = time.perf_counter()

    get_blurbs_to_download(
        snakemake.input[0],
        snakemake.output[0],
        snakemake.params.get("outdir", ""),
        max_threads,
    )

    print(
        "\nCode completed in {} sec(s).".format(time.perf_counter() - t),
        file=sys.stderr,
    )
