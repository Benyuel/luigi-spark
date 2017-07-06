import os.path
import sys
import inspect
sys.path.insert(0, os.path.abspath(".."))
import luigi
from luigi.contrib.simulate import RunAnywayTarget
from pipeline import SparkSubmitTask

repo = luigi.configuration.get_config().get('repo','dir')
deploy_mode = luigi.configuration.get_config().get('spark','deploy-mode')
queue = luigi.configuration.get_config().get('spark','queue')

#############################################################
# Base task class to use for creating tables/running arbitary sql
#############################################################
dir_sql = os.path.expanduser('{0}/sql/hadoop/'.format(repo))

def read_sql(table):
    lines = open(dir_sql + table + '.sql', 'r').readlines()
    # skip commented lines
    lines = [line for line in lines if not line.strip().startswith('--')]
    return ' '.join(lines)

class SQL(luigi.ExternalTask):
    context = luigi.Parameter(default='spark')
    deploy_mode = luigi.Parameter(default=deploy_mode)
    queue = luigi.Parameter(default=queue)
    db = luigi.Parameter(default=luigi.configuration.get_config().get('hive','db'))
    require_success = luigi.BoolParameter(default=True)

    def run(self):
        sql = 'use {0}; '.format(self.db)
        sql += read_sql(self.__class__.__name__)
        # if you don't require_success, the sql/table build can fail
        # and the rest of the workflow will continue anyway
        if bool(self.require_success):
            yield SparkSubmitTask(app=os.path.expanduser('~/spark-utils/hive.py'),
                                  entry_class='SparkSQLTask',
                                  app_options='--sql "{0}" --context {1}'.format(sql, self.context),
                                  deploy_mode=self.deploy_mode,
                                  queue=self.queue)
        else:
            SparkSubmitTask(app=os.path.expanduser('~/spark-utils/hive.py'),
                            entry_class='SparkSQLTask',
                            app_options='--sql "{0}" --context {1}'.format(sql, self.context),
                            deploy_mode=self.deploy_mode,
                            queue=self.queue).run()
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


#############################################################
# Each table has it's own class
# Each class is named according to it's table
# Each name has a corresponding sql/hadoop/name.sql def file
   # "" Double quotes must be escaped in .sql file
#############################################################

class test(SQL): pass


class test_master(SQL):

    def requires(self):
        return [test()]


class All(luigi.WrapperTask):

    def get_classes(self, ignore=['All', 'SQL']):
        all_classes = inspect.getmembers(sys.modules[__name__], lambda member: inspect.isclass(member) and member.__module__ == __name__)
        return [c[1] for c in all_classes if c[0] not in ignore]

    def requires(self):
        for table in self.get_classes():
            yield table()




if __name__ == '__main__':
    luigi.run()
