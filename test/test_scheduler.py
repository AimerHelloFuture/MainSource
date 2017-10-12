#!/usr/bin/env python
# -*- coding: utf-8 -*-

from apscheduler.schedulers.blocking import BlockingScheduler
import datetime


def work():
    print 'hello'

scheduler = BlockingScheduler()
scheduler.add_job(work, 'interval', seconds=5)

try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()

