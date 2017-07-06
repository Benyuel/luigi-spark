import os
import time
import socket
import luigi
from luigi.cmdline_parser import CmdlineParser
import configparser

DEFAULT_LUIGI_CONFIG_PATH_HDFS = luigi.configuration.get_config().get('spark','config_hdfs')
DEFAULT_LUIGI_CONFIG_PATH_LOCAL = os.path.expanduser(luigi.configuration.get_config().get('spark','config_local'))
DEFAULT_WAREHOUSE_DIR = DEFAULT_LUIGI_CONFIG_PATH_HDFS.replace('luigi.cfg','').replace('hdfs:','')
DEFAULT_SCHEDULER = luigi.configuration.get_config().get('scheduler','host')
DEFAULT_PORT = luigi.configuration.get_config().get('scheduler','port')

###############################################
# Luigi config parser
###############################################
def read_hdfs_config(spark, path=DEFAULT_LUIGI_CONFIG_PATH_HDFS):
    print('reading config from hdfs:{0}'.format(path))
    config_df = spark.read.text(path)
    config_array = [i[0] for i in config_df.collect()]
    config_str = '\n'.join(config_array)
    config = configparser.ConfigParser()
    config.read_string(config_str)
    return config

def read_local_config(path=DEFAULT_LUIGI_CONFIG_PATH_LOCAL):
    config = configparser.ConfigParser()
    config.read(path)
    return config

###############################################
# Luigi cmdline parser
###############################################
def get_task(cmdline_args):
    with CmdlineParser.global_instance(cmdline_args) as cp:
        return cp.get_task_obj()

###############################################
# not thread safe
###############################################
def build(tasks, worker_scheduler_factory=None, **env_params):
    """
    Builds workflow programmatically
    Example:
             luigi.build([MyTask1(), MyTask2()], local_scheduler=True)
    `build` defaults to not using the identical process lock. 
    :return: True if there were no scheduling errors, even if tasks may fail.
    """
    if "no_lock" not in env_params:
        env_params["no_lock"] = True

    return luigi.interface._schedule_and_run(tasks, worker_scheduler_factory, override_defaults=env_params)

###############################################
# thread safe
###############################################
def port_is_open(port, host='127.0.0.1'):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((host, port))
    return result == 0

def run(task, scheduler='127.0.0.1', port=8082):
    """ Thread safe alternative of luigi.build """
    if port_is_open(port):
        sch = luigi.rpc.RemoteScheduler(url='http://{0}:{1}'.format(scheduler,str(port)))
    else:
        sch = luigi.scheduler.Scheduler()
    # no_install_shutdown_handler makes it thread safe
    w = luigi.worker.Worker(scheduler=sch, no_install_shutdown_handler=True)
    w.add(task)
    w.run()

###############################################
# thread safe
###############################################
def build_and_run(cmdline_args, unique_id=None, scheduler=DEFAULT_SCHEDULER, port=DEFAULT_PORT):
    task = get_task(cmdline_args)
    if unique_id:
        task.unique_id = unique_id
    return run(task, scheduler=scheduler, port=port)

###############################################
# decorator interfaces for luigi task statuses
###############################################
@luigi.Task.event_handler(luigi.Event.FAILURE)
def failure(task, exception):
    print('\n-- ALERT --')
    # try/except to prevent blocking failure of a task
    try: print('{0} FAILED'.format(task.unique_id))
    except: print('FAILED: Exception')
    print('Task {0} FAILED at time {1} GMT...'.format(task,time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())))
    print(exception)
    print('-- ALERT --\n')

@luigi.Task.event_handler(luigi.Event.START)
def start(task):
    print('{0} STARTED'.format(task.unique_id))
    print('Starting task {0} at time {1} GMT...'.format(task,time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())))

@luigi.Task.event_handler(luigi.Event.SUCCESS)
def sucess(task):
    print('{0} SUCCEEDED'.format(task.unique_id))
    print('Task {0} succeeded at time {1} GMT...'.format(task,time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())))

@luigi.Task.event_handler(luigi.Event.BROKEN_TASK)
def broken_task(task, exception):
    print('\n-- ALERT --')
    print('Task {0} BROKEN at time {1} GMT...'.format(task,time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())))
    print(exception)
    print('-- ALERT --\n')

@luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
def processing_time(task, processing_time):
    print('Processing time for task {0} was {1} seconds'.format(task,processing_time))
    print('Processing time for task {0} was {1} minutes'.format(task,processing_time/60))
    print('Processing time for task {0} was {1} hours'.format(task,processing_time/60/60))

@luigi.Task.event_handler(luigi.Event.DEPENDENCY_MISSING)
def dependency_missing(task):
    print('Task {0} missing as dependency'.format(task))


@luigi.Task.event_handler(luigi.Event.DEPENDENCY_DISCOVERED)
def dependency_discovered(task, dependency):
    print('Task {0} depends on Task {1}'.format(task, dependency))



