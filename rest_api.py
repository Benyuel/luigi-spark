from pyspark.sql import SparkSession
# from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys
import datetime
import calendar
import time
import requests
import subprocess
import luigi
from luigi.contrib.simulate import RunAnywayTarget
import pandas as pd
# to handle importing via --py-files vs package management
try:
    from interface import build_and_run, read_hdfs_config, DEFAULT_WAREHOUSE_DIR
    import hive
except:
    from spark_utils.interface import build_and_run, read_hdfs_config, DEFAULT_WAREHOUSE_DIR
    from spark_utils import hive

spark = SparkSession \
      .builder \
      .appName("Luigi on Spark - Zendesk") \
      .config("hive.metastore.warehouse.dir", DEFAULT_WAREHOUSE_DIR) \
      .config("spark.driver.extraClassPath", "/usr/hdp/current/hadoop-client/lib/hadoop-lzo-0.6.0.2.2.0.0-2041.jar") \
      .config("spark.driver.extraLibraryPath", "/usr/hdp/current/hadoop-client/lib/native:/usr/hdp/current/hadoop-client/lib/native/Linux-amd64-64") \
      .config("spark.yarn.submit.waitAppCompletion", "true") \
      .enableHiveSupport() \
      .getOrCreate()

config = read_hdfs_config(spark)


###################################################################
# Class that can pull data from various zendesk api's
###################################################################
class Client:
    def __init__(self, url, user, token):
        self.url = url
        self.user = user
        self.token = token

    def get(self, endpoint):
        headers = {'Accept': 'application/json'}
        url = self.url + endpoint
        response = requests.get(url, auth=(self.user, self.token), headers=headers)
        return response

    def status_handler(self, response):
        assert response.status_code == 200, print('error: status-code {0}'.format(response.status_code))


###################################################################
# Generic class to ETL Zendesk data from API to Hive via Spark
# Subclass this and create a method with the same name as your table which
# return a spark df of your data to Load
###################################################################
class API(luigi.ExternalTask):
    url = luigi.Parameter()
    endpoint = luigi.Parameter()
    user = luigi.Parameter()
    token = luigi.Parameter()
    table = luigi.Parameter()
    hive_db = luigi.Parameter(default='default')

    def read(self):
        client = Client(url=self.url, user=self.user, token=self.token)
        response = client.get(endpoint=self.endpoint)
        client.status_handler(response)
        result = response.json()
        df = pd.DataFrame(result)
        self.df = spark.createDataFrame(df)
        if self.df.count() == 0:
            self.output().done()

    def write(self):
        spark.sql("USE {0}".format(self.hive_db))
        self.df.registerTempTable("{0}".format(self.table))
        spark.sql("CREATE TABLE {0} AS SELECT * FROM {0}".format(self.table))

    def run(self):
        self.read()
        self.write()
        self.output().done()

    # output run anyway temp file
    def output(self):
        return RunAnywayTarget(self)




if __name__ == '__main__':
    cmdline_args = sys.argv[1:]
    # if there was a unique identifier passed we remove from the arguments and pass separately
    unique_id = None
    if '--unique-identifier' in cmdline_args:
        index = cmdline_args.index('--unique-identifier')
        flag = cmdline_args.pop(index)
        unique_id = cmdline_args.pop(index)

    build_and_run(cmdline_args, unique_id=unique_id)
