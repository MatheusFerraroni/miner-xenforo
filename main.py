import logging
import argparse
import logging
import time

from MinerXenForo import Manager



def main(base_url, reload_threads, max_request, cache_pages, summary, reload_posts):
    args = locals()

    FORMAT = '%(asctime)s %(levelname)s %(funcName)s %(threadName)s - \t %(message)s'
    logging.basicConfig(filename='log_minerxenforo.log', filemode='a', format=FORMAT, level=logging.INFO)

    logging.info('Starting')
    logging.info(str(args))

    mng = Manager(base_url, max_request, cache_pages)

    if summary:
        mng.print_summary()
        print("\n*When the summary is shown nothing else is done.")
        return
    else:
        if reload_threads:
            mng.reload_threads()

        if reload_posts:
            mng.reload_posts()


if __name__ == "__main__":


    ap = argparse.ArgumentParser()

    ap.add_argument("-url", required=True, type=str, help="The base URL of the forum using XenForo")
    ap.add_argument("-s", "--summary", required=False, action="store_true", help="Summary")
    ap.add_argument("-rt", "--reload-threads", required=False, action="store_true", help="If present the miner will search for new threads")
    ap.add_argument("-rp", "--reload-posts", required=False, action="store_true", help="If present the miner will search for new posts in previously detected threads")
    ap.add_argument("-cp", "--cache-pages", required=False, action="store_true", help="Cache HTML requests. High storage memmory usage")
    ap.add_argument("-mr", "--max-request", required=False, default=1, type=int, help="Maximum simultaneous request")

    args = vars(ap.parse_args())

    start = time.time()

    main(args['url'], args['reload_threads'], args['max_request'], args['cache_pages'], args['summary'], args['reload_posts'])

    end = time.time()
    total = end-start
    print("Total time: {}".format(total))

    logging.info("Stopping. Total time: {}".format(total))