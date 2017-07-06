# spreadsheet must be shared with a google service account with credentials in your config
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import sys
import json
import luigi
from luigi.contrib.simulate import RunAnywayTarget
import pandas as pd
from oauth2client.client import SignedJwtAssertionCredentials
import gspread
import base64
# to handle importing via --py-files vs package management
try:
    from interface import build_and_run, read_hdfs_config, DEFAULT_WAREHOUSE_DIR
    import hive
except:
    from spark_utils.interface import build_and_run, read_hdfs_config, DEFAULT_WAREHOUSE_DIR
    from spark_utils import hive

spark = SparkSession \
      .builder \
      .appName("Luigi on Spark - Google") \
      .config("hive.metastore.warehouse.dir", DEFAULT_WAREHOUSE_DIR) \
      .enableHiveSupport() \
      .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)
config = read_hdfs_config(spark)

class Sheet(luigi.ExternalTask):
    sheet_name = luigi.Parameter()
    tab_name = luigi.Parameter()
    hive_db = luigi.Parameter()
    hive_table = luigi.Parameter()
    overwrite = luigi.Parameter(default=True)

    def auth(self):
        scope = [config['google_sheets']['scope']]
        client_email = config['google_sheets']['client_email']
        private_key = config['google_sheets']['private_key']
        credentials = SignedJwtAssertionCredentials(client_email, private_key.encode(), scope)
        self.gc = gspread.authorize(credentials)

    def read(self):
        sheet = self.gc.open(self.sheet_name)
        locsheet = sheet.worksheet(self.tab_name)
        data = locsheet.get_all_values()
        header = locsheet.get_all_values()[0]
        header = [col.strip().lower().replace(' ','_').replace('/','_') for col in header]
        data = [l for l in data if l != header]
        df = pd.DataFrame(data, columns = header)
        self.df = sqlContext.createDataFrame(df)

    def write(self):
        sqlContext.sql("USE {0}".format(self.hive_db))
        self.df.registerTempTable('{0}_temp'.format(self.hive_table))
        if self.overwrite:
            sqlContext.sql("DROP TABLE IF EXISTS {0}".format(self.hive_table))
        sqlContext.sql("CREATE TABLE {0} AS SELECT * FROM {0}_temp".format(self.hive_table))

    def run(self):
        self.auth()
        self.read()
        self.write()
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


class SheetToHDFS(Sheet):
    sheet_name = luigi.Parameter()
    tab_name = luigi.Parameter()
    path = luigi.Parameter()
    delimiter = luigi.Parameter(default='|')

    def write(self):
        self.df.write.option("header","true").option("delimiter",self.delimiter).csv(self.path)



if __name__ == '__main__':
    cmdline_args = sys.argv[1:]
    #if there was a unique identifier passed we remove from the arguments and pass separately
    unique_id = None
    if '--unique-identifier' in cmdline_args:
        index = cmdline_args.index('--unique-identifier')
        flag = cmdline_args.pop(index)
        unique_id = cmdline_args.pop(index)

    build_and_run(cmdline_args, unique_id=unique_id)
