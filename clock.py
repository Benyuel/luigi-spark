################################################################################
# safe & multithreaded programmatic version of crontab
################################################################################
# def job():
#     print("I'm working")

# @catch_exceptions
# def badjob():
#     return 1 / 0

# def worker_main():
#     while 1:
#         job_func = jobqueue.get()
#         job_func()

# scheduler.every(10).seconds.do(jobqueue.put, job).tag('daily-tasks','test1')
# scheduler.every(10).seconds.do(jobqueue.put, job).tag('daily-tasks','test1')
# scheduler.every(10).seconds.do(jobqueue.put, job).tag('daily-tasks','test3')
# scheduler.every(10).seconds.do(jobqueue.put, job).tag('daily-tasks','test4')
# scheduler.every(10).seconds.do(jobqueue.put, badjob).tag('daily-tasks','badtest')
# schedule.clear('daily-tasks')
#################################################################################

import functools
import queue
import time
import threading
import schedule

from interface import run
from pipeline import SparkSubmitTask

scheduler = schedule.Scheduler()
jobqueue = queue.Queue()

def catch_exceptions(job_func, cancel_on_failure=False):
    @functools.wraps(job_func)
    def wrapper(*args, **kwargs):
        try:
            return job_func(*args, **kwargs)
        except:
            import traceback
            print(traceback.format_exc())
            if cancel_on_failure:
                return schedule.CancelJob
    return wrapper

def worker_main():
    while 1:
        job_func = jobqueue.get()
        job_func()

# Define your jobs here
def test_job():
    print('This is a test to be run at 13:00 every 1 day')

# Define your schedule(s) here
scheduler.every(1).day.at("13:00").do(jobqueue.put, test_job).tag('test','job')

# Start your worker processes here (you can add multiple)
# worker_thread = threading.Thread(target=worker_main)
# worker_thread.start()
n = 10
worker_threads = [threading.Thread(target=worker_main) for i in range(0,n)]
[thread.start() for thread in worker_threads]

try:
    # to insert yourself into the clock to an interactive shell hit Ctrl-C
    try:
        while 1:
            # schedule.run_pending()
            # to run the scheduler in a nonblocking separate thread
            scheduler.run_continuously()
            time.sleep(1)
    except KeyboardInterrupt:
        import pdb; pdb.set_trace()
except:
    print('error in clock loop...\nPLEASE RESTART NOW...\n')
