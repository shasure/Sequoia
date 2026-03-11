# -*- encoding: UTF-8 -*-

import utils
import logging
import work_flow_a
import settings
import schedule
import time
import datetime
from pathlib import Path
from multiprocessing import freeze_support

import work_flow_hk


def job():
    if utils.is_weekday():
        work_flow_a.prepare_a()
        # work_flow_hk.prepare_hk()


if __name__ == '__main__':
    freeze_support()

    logging.basicConfig(format='%(asctime)s %(message)s', filename='sequoia.log')
    logging.getLogger().setLevel(logging.INFO)
    settings.init()

    if settings.config['cron']:
        EXEC_TIME = "16:10"
        schedule.every().day.at(EXEC_TIME).do(job)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        work_flow_a.prepare_a()
        # work_flow_hk.prepare_hk()
