import os.path
import sys
sys.path.insert(0, os.path.abspath(".."))
import luigi
from pipeline import SparkSubmitTask

repo = luigi.configuration.get_config().get('repo','dir')
deploy_mode = luigi.configuration.get_config().get('spark','deploy-mode')
queue = luigi.configuration.get_config().get('spark','queue')

class Run(luigi.WrapperTask):
    sheet_name = luigi.Parameter()
    tab_name = luigi.Parameter()
    hive_table = luigi.Parameter()
    hive_db = luigi.Parameter(default=luigi.configuration.get_config().get('hive','db'))

    def requires(self):
        yield SparkSubmitTask(app='{0}/google_sheet.py'.format(repo),
                              entry_class='Sheet',
                              app_options='''--sheet-name '{0}' --tab-name '{1}' --hive-db {2} --hive-table {3}'''.format(self.sheet_name, self.tab_name, self.hive_db, self.hive_table),
                              deploy_mode=deploy_mode,
                              queue=queue,
                              local=False)




if __name__ == '__main__':
  luigi.run()