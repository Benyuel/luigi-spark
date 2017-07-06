#################################################################
# spark utils for submitting jobs and managing spark environments
#################################################################
import logging
import os
import getpass
import luigi
from luigi.contrib.simulate import RunAnywayTarget
import subprocess
import shutil
import requests
import lxml, lxml.html
import uuid
from setup import console, PYTHON_DIR, REQUIREMENTS_PIP, REQUIREMENTS_CONDA

logger = logging.getLogger('luigi-interface')

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class SetUpError(Exception):
    def __init__(self, msg):
        self.msg = msg

###############################################################
# Methods to check yarn for job status / logs
###############################################################

def yarn_apps(user=getpass.getuser(), rm=luigi.configuration.get_config().get('yarn','rm')):
    apps = requests.get('{0}/ws/v1/cluster/apps?user={1}'.format(rm, user)).json()
    return sorted(apps['apps']['app'], key=lambda x: x['finishedTime'], reverse=True)

def yarn_app(id, user=getpass.getuser(), rm=luigi.configuration.get_config().get('yarn','rm')):
    return requests.get('{0}/ws/v1/cluster/apps/{1}?user={2}'.format(rm, id, user)).json()

def yarn_app_logs(id, user=getpass.getuser(), rm=luigi.configuration.get_config().get('yarn','rm')):
    app_json = yarn_app(id)
    assert 'app' in app_json, 'App does not exist...'
    assert 'amContainerLogs' in app_json['app'], 'Logs do not exist...'
    try: return yarn_logs(app_json['app']['amContainerLogs'])
    except: return yarn_logs(app_json['app']['amContainerLogs'], handle_redirect=True)

def yarn_logs(url, 
              user=getpass.getuser(),
              rm=luigi.configuration.get_config().get('yarn','rm'), 
              job_history_port=luigi.configuration.get_config().get('yarn','job_history_port'), 
              log_container_port=luigi.configuration.get_config().get('yarn','log_container_port'), 
              handle_redirect=False, 
              types=['launch_container.sh', 'directory.info', 'stderr', 'stdout']):
    # handle redirect for currently running jobs
    if handle_redirect: 
        logs = requests.get(url)
        html = lxml.html.fromstring(logs.text)
        redirect_url = None
        try:
            redirect_url = html.cssselect('meta[http-equiv="refresh"]')[0].get('content').split("url=")[1]
        except IndexError as e:
            return None
        return {t:requests.get(redirect_url + '/' + t + '/?start=0').text for t in types}
    else:
        logs_node = url.replace('http://','').split(':')[0]
        logs_container = url.split('/')[-2]
        return {t:requests.get(rm.rsplit(':',1)[0] + ':' + job_history_port + '/jobhistory/logs/' + logs_node + ':' + log_container_port + '/' + logs_container + '/' + logs_container + '/' + user + '/' + t + '/?start=0').text for t in types}

def yarn_app_status(unique_identifier, log_types=['stderr', 'stdout']):
    for app in yarn_apps():
        app_logs = yarn_app_logs(app['id'])
        for t in log_types:
            if '{0} FAILED'.format(unique_identifier) in app_logs[t]:
                raise RuntimeError(app_logs[t])
                return 'FAILURE'
            elif '{0} SUCCEEDED'.format(unique_identifier) in app_logs[t]:
                return 'SUCCESS'
            elif 'Exception' in app_logs[t]:
                return 'FAILURE'
            elif unique_identifier in app_logs[t]:
                return app['finalStatus']


###############################################################
# Task to create the spark python environment
###############################################################
class PythonENV(luigi.ExternalTask):
    packages = luigi.ListParameter(default=REQUIREMENTS_PIP.split(' '))
    packages_conda = luigi.ListParameter(default=REQUIREMENTS_CONDA.split(' '))
    python_version = luigi.Parameter(default='3.5')
    env_name = luigi.Parameter(default=luigi.configuration.get_config().get('python','env'))
    overwrite = luigi.BoolParameter(default=True)

    def remove(self):
        if os.path.isfile(os.path.expanduser('{0}/envs/{1}.zip'.format(PYTHON_DIR, self.env_name))):
            print('env {0}.zip already exists...removing...and...recreating...'.format(self.env_name))
            os.remove(os.path.expanduser('{0}/envs/{1}.zip'.format(PYTHON_DIR, self.env_name)))
        if os.path.exists(os.path.expanduser('{0}/envs/{1}'.format(PYTHON_DIR, self.env_name))):
            print('env {0} already exists...removing...and...recreating...'.format(self.env_name))
            shutil.rmtree(os.path.expanduser('{0}/envs/{1}'.format(PYTHON_DIR, self.env_name)))

    def create(self):
        cmd = 'conda create -n {0} --copy -y -q python={1}'.format(self.env_name,self.python_version)
        return_code, stdout, stderr = console(cmd)
        assert return_code == 0, 'Cannot create python env...'
        cmd_base_env = 'source activate {0} && '.format(self.env_name)
        for req in self.packages:
            return_code, stdout, stderr = console(cmd_base_env + '/usr/bin/yes | pip install {0}'.format(req))
            if return_code != 0:
                return_code, stdout, stderr = console(cmd_base_env + '/usr/bin/yes | conda install {0}'.format(req))
            assert return_code == 0, 'Could not install package: {0}'.format(req)
        for req in self.packages_conda:
            return_code, stdout, stderr = console(cmd_base_env + '/usr/bin/yes | conda install {0}'.format(req))
            assert return_code == 0, 'Could not install package using conda: {0}'.format(req)

        print('installing this spark_utils...')
        return_code, stdout, stderr = console(cmd_base_env + 'cd {0} && python setup.py install'.format(os.path.expanduser('~/spark-utils/')))
        assert return_code == 0, 'Cannot install spark_utils...FAILED'
        if self.overwrite:
            self.output().done()

    def package(self):
        cmd = 'cd {0}/envs/ && '.format(PYTHON_DIR)
        cmd += 'zip -r {0}.zip {0}'.format(self.env_name)
        return_code, stdout, stderr = console(cmd)
        if return_code != 0:
            raise SetUpError('Could not zip the python env...')

    def run(self):
        self.remove()
        self.create()
        self.package()
  
    def output(self):
        if self.overwrite:
            return RunAnywayTarget(self)
        else:
            # done once the python env zipped is done
            return luigi.local_target.LocalTarget(path=os.path.expanduser("{0}/envs/{1}.zip".format(PYTHON_DIR,self.env_name)))


###############################################################
# Task to submit a spark job on Hortonworks stack
###############################################################
class SparkSubmitTask(luigi.ExternalTask):
    # Application (.jar or .py file)
    app = luigi.Parameter()
    entry_class = luigi.Parameter(default='')
    app_options = luigi.Parameter(default='')
    spark_path = luigi.Parameter(default=luigi.configuration.get_config().get('spark','home'))
    master = luigi.Parameter(default=luigi.configuration.get_config().get('spark','master'))
    deploy_mode = luigi.Parameter(default=luigi.configuration.get_config().get('spark','deploy-mode'))
    queue = luigi.Parameter(default=luigi.configuration.get_config().get('spark','queue'))
    confs = luigi.Parameter(default='') # ['x=y','z=a', ...]
    archives = luigi.Parameter(default='')
    executor_memory = luigi.Parameter(default='')
    driver_memory = luigi.Parameter(default='')
    num_executors = luigi.Parameter(default='')
    executor_cores = luigi.Parameter(default='')
    jars = luigi.Parameter(default=luigi.configuration.get_config().get('spark','jars'))
    packages = luigi.Parameter(default='') #https://mvnrepository.com/
    files = luigi.Parameter(default=luigi.configuration.get_config().get('spark','files'))
    py_files = luigi.Parameter(default='')
    driver_class_path = luigi.Parameter(default='')
    # Only log stderr if spark fails (since stderr is normally quite verbose)
    python_env = luigi.Parameter(default=luigi.configuration.get_config().get('python','env'))
    python_force_build_env = luigi.BoolParameter(default=False)
    local = luigi.BoolParameter(default=False)
    luigi_cfg_path = luigi.Parameter(default=luigi.configuration.get_config().get('spark','config_local'))
    always_log_stderr = False

    def env(self):
        if self.deploy_mode == 'client':
            cmd = 'cd {0} && '.format(PYTHON_DIR)
            assert os.path.exists(os.path.expanduser('{0}/envs/{1}/bin/python'.format(PYTHON_DIR,self.python_env)))
            self.python_path = './envs/{0}/bin/python'.format(self.python_env)
            self.py_archives = 'envs/{0}.zip#envs'.format(self.python_env)
        elif self.deploy_mode == 'cluster':
            cmd = 'cd {0}/envs/ && '.format(PYTHON_DIR)
            self.python_path = './ENV/{0}/bin/python'.format(self.python_env)
            self.py_archives = '{0}.zip#ENV'.format(self.python_env)
        cmd += 'export PYSPARK_PYTHON="{0}" && '.format(self.python_path)
        if bool(self.archives):
            self.archives += ',' + self.py_archives
        else:
            self.archives = self.py_archives
        return cmd

    def bash_rc(self):
        return 'source {0} && '.format(os.path.expanduser('~/.bashrc'))

    def spark_home(self):
        cmd = 'export SPARK_HOME={0} && '.format(self.spark_path)
        return cmd

    def luigi_cfg(self):
        cmd = 'export LUIGI_CONFIG_PATH={0} && '.format(self.luigi_cfg_path)
        return cmd

    @property
    def spark_submit(self):
        cmd = 'export SPARK_HOME={0} && '.format(self.spark_path)
        return cmd + self.spark_path + '/bin/spark-submit '

    def run(self):
        cmd = self.bash_rc()
        if bool(self.python_env):
            if bool(self.python_force_build_env):
                yield PythonENV(env_name=self.python_env)
            cmd += self.env()
        cmd += self.luigi_cfg()
        cmd += self.spark_submit
        cmd += '--master {0} '.format(self.master)
        cmd += '--deploy-mode {0} '.format(self.deploy_mode)
        cmd += '--queue {0} '.format(self.queue)
        if bool(self.python_env):
            cmd += ' --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON={0} '.format(self.python_path)
        if bool(self.confs):
            cmd += '--conf ' + ' --conf '.join(self.confs.split(' '))
        if bool(self.archives):
            cmd += ' --archives {0} '.format(self.archives)
        if bool(self.packages):
            cmd += ' --packages {0} '.format(self.packages)
        if bool(self.executor_memory):
            cmd += ' --executor-memory {0} '.format(self.executor_memory)
        if bool(self.driver_memory):
            cmd += ' --driver-memory {0} '.format(self.driver_memory)
        if bool(self.num_executors):
            cmd += ' --num-executors {0} '.format(self.num_executors)
        if bool(self.executor_cores):
            cmd += ' --executor-cores {0} '.format(self.executor_cores)
        if bool(self.jars):
            cmd += ' --jars {0} '.format(self.jars)
        if bool(self.files):
            cmd += ' --files {0} '.format(self.files)
        if bool(self.py_files):
            cmd += ' --py-files {0} '.format(self.py_files)
        if bool(self.driver_class_path):
            cmd += ' --driver-class-path {0} '.format(self.driver_class_path)
        cmd += self.app
        if bool(self.entry_class):
            cmd += ' {0}'.format(self.entry_class)
        if bool(self.app_options):
            cmd += ' ' + self.app_options
        #Here we are going to add a randomly generated string as a parameter to uniquely identify the logs
        unique_identifier = uuid.uuid4().hex
        cmd += ' --unique-identifier {0} '.format(unique_identifier)
        if bool(self.local):
            cmd += ' --local-scheduler'

        #Clean up the .Trash folder which will otherwise take up disk space
        subprocess.call("hadoop fs -rm -r .Trash", shell=True)

        print('-------\nSUBMITTING:\n\n{0}\n\n-------'.format(cmd))
        return_code, stdout, stderr = console(cmd)

        #Clean up the .Trash folder which will otherwise take up disk space
        subprocess.call("hdfs dfs -rm -r -skipTrash .Trash", shell=True)
        
        if yarn_app_status(unique_identifier) == 'SUCCESS':
            self.output().done()

    def output(self):
        return RunAnywayTarget(self)




if __name__ == '__main__':
    luigi.run()