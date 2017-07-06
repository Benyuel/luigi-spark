#################################################################
# setting up this repo
#################################################################

import os
import sys
import site
import subprocess
import shutil
import setuptools
import distutils.cmd
import distutils.log
import ConfigParser

Config = ConfigParser.ConfigParser()
Config.read('luigi.cfg')

####### DEFAULTS #########
PYTHON_DIR = Config.get('python','dir')
REQUIREMENTS_PIP = Config.get('python_requirements','pip')
REQUIREMENTS_CONDA = Config.get('python_requirements','conda')
##########################
    
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def console(cmd):
    # TODO - standardize error outputs to be useful
    try: 
        status = subprocess.call(cmd, shell=True)
        return status, None, None
    except:
        status = subprocess.run(cmd, shell=True)
        return status.returncode, status.stdout, status.stderr


class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class SetUpError(Exception):
    def __init__(self, msg):
        self.msg = msg

###############################################################
# Task to install python
###############################################################
class Python(distutils.cmd.Command):
    description = 'installs python in your home dir'
    user_options = [('python-version', 'v', ''),
                    ('installer','i',''),
                    ('profile','p','')]

    def initialize_options(self):
        self.python_version = Config.get('python','version')
        self.installer = Config.get('python','installer')
        self.profile = Config.get('bash','profile')

    def finalize_options(self):
        assert self.python_version == '3.5.2', "This is only version tested against..."

    def run(self):
        cmd = '/usr/bin/wget {0} && '.format(self.installer)
        cmd += '/bin/chmod 755 {0} && '.format(self.installer.split('/')[-1])
        cmd += './{0}'.format(self.installer.split('/')[-1])
        self.announce('installing python {0}'.format(self.python_version), 
                      level=distutils.log.INFO)
        return_code = console(cmd)
        assert return_code[0] == 0, 'failed installation...'
        self.install_requirements()
        print('python installation complete!')

    def install_requirements():
        for req in REQUIREMENTS_PIP.split(' '):
            console('/usr/bin/yes | pip install {0}'.format(req))
        for req in REQUIREMENTS_CONDA:
            console('/usr/bin/yes | conda install {0}'.format(req))

     # def output(self):
     #    # done installing if anaconda dir exists
     #    return os.path.isdir(os.path.expanduser(PYTHON_DIR))

###############################################################
# Task to install this repo as a package
###############################################################
class Install(distutils.cmd.Command):
    description = 'installs this repo as a python package'
    user_options = [('packages-path', 'p', 'site-packages-dir'),
                    ('overwrite', 'o', 'do-not-overwrite-existing-installation')]

    def initialize_options(self):
        self.packages_path = site.getsitepackages()[0]
        self.overwrite = 'True'

    def finalize_options(self):
        assert os.path.exists(self.packages_path)

    def run(self):
        if os.path.exists(self.packages_path + '/' + Config.get('repo','name')):
            if self.overwrite == 'True':
                shutil.rmtree(self.packages_path + '/' + Config.get('repo','name'))
            else:
                raise FileExistsError('spark_utils already installed...rerun with overwrite=True')
        result = shutil.copytree(os.path.expanduser(Config.get('repo','dir')), self.packages_path + '/' + Config.get('repo','name'))
        assert result == self.packages_path + '/' + Config.get('repo','name'), 'Failed to install copying .py files...'
        print('spark-utils installation complete!')
        print('you can now import spark-utils as spark_utils in your python environment...')
        print('\n\nsetting up spark-utils/bin/ executables...\n\n')
        console('chmod +x {0}/*'.format(os.path.expanduser(Config.get('repo','dir') + '/bin')))




if __name__ == '__main__':
    setuptools.setup(name=Config.get('repo','name'),
                     version='0.1',
                     py_modules=[f.replace('.py','') for f in os.listdir(os.path.expanduser(Config.get('repo','dir'))) if '.py' in f],
                     author="Ben Pleasanton & Tom Eddy",
                     description="Layered Luigi Data Pipelines using pyspark",
                     long_description=read('README.md'),
                     cmdclass = {'python': Python,
                                 'install': Install},
                     )