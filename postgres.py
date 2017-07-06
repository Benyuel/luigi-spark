from pyspark.sql import SparkSession
import sys
import luigi
from luigi.contrib.simulate import RunAnywayTarget
# to handle importing via --py-files vs package management
try:
    from interface import build_and_run, read_hdfs_config, DEFAULT_WAREHOUSE_DIR
    import hive
except:
    from spark_utils.interface import build_and_run, read_hdfs_config, DEFAULT_WAREHOUSE_DIR
    from spark_utils import hive

spark = SparkSession \
      .builder \
      .appName("Luigi on Spark - Postgres") \
      .config("hive.metastore.warehouse.dir", DEFAULT_WAREHOUSE_DIR) \
      .config("spark.driver.extraClassPath", "/usr/hdp/current/hadoop-client/lib/hadoop-lzo-0.6.0.2.2.0.0-2041.jar") \
      .config("spark.driver.extraLibraryPath", "/usr/hdp/current/hadoop-client/lib/native:/usr/hdp/current/hadoop-client/lib/native/Linux-amd64-64") \
      .config("spark.yarn.submit.waitAppCompletion", "true") \
      .enableHiveSupport() \
      .getOrCreate()

config = read_hdfs_config(spark)

###################################################################
# Class that creates/loads the hive table and dedupes if necessary
###################################################################
class Table(luigi.ExternalTask):
    table = luigi.Parameter()
    hive_db = luigi.Parameter()
    primary_key = luigi.Parameter(default=None)
    timestamp_field = luigi.Parameter(default=None)
    incremental = luigi.BoolParameter(default=False)
    host = luigi.Parameter(default=config.get('postgres','host'))
    port = luigi.Parameter(default=config.get('postgres','port'))
    db = luigi.Parameter(default=config.get('postgres','database'))
    user = luigi.Parameter(default=config.get('postgres','user'))
    password = luigi.Parameter(default=config.get('postgres','password'))
    filters = luigi.Parameter(default="")
    dest_suffix = luigi.Parameter(default="")
    jdbc_ssl = luigi.BoolParameter(default=False)

    def read(self):
        jdbcURL = "jdbc:postgresql://{0}:{1}/{2}?user={3}&password={4}{5}".format(
                     self.host,
                     self.port,
                     self.db,
                     self.user,
                     self.password,
                     "&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory" if self.jdbc_ssl else "")
        if self.incremental:
            latest = hive.latest_timestamp(self.table + self.dest_suffix, self.hive_db, self.timestamp_field, spark)
            to_add = "{0} >= TIMESTAMP '{1}'".format(self.timestamp_field, latest)
            if self.filters:
                self.filters += " AND {0}".format(to_add)
            else:
                self.filters = to_add
        self.df = spark.read.format("jdbc").options(url=jdbcURL,dbtable="(SELECT * FROM {0} {1} ) sub".format(self.table, 'WHERE ' + self.filters if self.filters else ""), driver='org.postgresql.Driver').load()
        if self.df.limit(10).count() < 1:
            self.output().done()

    def write(self):
        spark.sql("USE {0}".format(self.hive_db))
        if self.incremental:
            hive.drop("{0}_temp{1}".format(self.table, self.dest_suffix), self.hive_db, spark)
            self.df.registerTempTable("{0}_tmp".format(self.table))
            spark.sql("CREATE TABLE {0}_temp{1} AS SELECT * FROM {0}_tmp".format(self.table,self.dest_suffix))
            hive.dedupe(base_table=self.table + self.dest_suffix, 
                        temp_table="{0}_tmp".format(self.table), 
                        reconcile_table="{0}_reconcile{1}".format(self.table, self.dest_suffix), 
                        db=self.hive_db, 
                        timestamp_field=self.timestamp_field, 
                        primary_key=self.primary_key, 
                        spark=spark)
        else:
            hive.drop("{0}{1}".format(self.table, self.dest_suffix), self.hive_db, spark)
            self.df.registerTempTable("{0}_tmp".format(self.table))
            spark.sql("CREATE TABLE {0}{1} AS SELECT * FROM {0}_tmp".format(self.table, self.dest_suffix))

    def run(self):
        if (self.incremental) & (not hive.exists(self.table + self.dest_suffix, self.hive_db, spark)):
            task = Table(table=self.table + self.dest_suffix,
                         hive_db=self.hive_db,
                         host=self.host,
                         port=self.port,
                         db=self.db,
                         user=self.user,
                         password=self.password,
                         filters=self.filters,
                         dest_suffix=self.dest_suffix,
                         jdbc_ssl=self.jdbc_ssl) 

            if self.unique_id:
                task.unique_id = self.unique_id
            #Build the incremental table from scratch if it does not exist
            yield task 
        self.read()
        self.write()
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


##################################################################
# Class that executes postgresql query and saves to Hive table
##################################################################
class Query(luigi.ExternalTask):
    query = luigi.Parameter()
    hive_table = luigi.Parameter()
    hive_db = luigi.Parameter()
    hive_overwrite = luigi.BoolParameter()
    host = luigi.Parameter(default=config.get('postgres','host'))
    port = luigi.Parameter(default=config.get('postgres','port'))
    db = luigi.Parameter(default=config.get('postgres','database'))
    user = luigi.Parameter(default=config.get('postgres','user'))
    password = luigi.Parameter(default=config.get('postgres','password'))

    def read(self):
        jdbcURL = "jdbc:postgresql://{0}:{1}/{2}?user={3}&password={4}&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory".format(
                     self.host,
                     self.port,
                     self.db,
                     self.user,
                     self.password)
        self.df = spark.read.format("jdbc").options(url=jdbcURL,dbtable="({0}) sub".format(self.query)).load()
        if self.df.rdd.isEmpty():
            self.output().done()

    def write(self):
        spark.sql("USE {0}".format(self.hive_db))
        if self.hive_overwrite:
            hive.drop(self.hive_table, self.hive_db, spark)

        #self.df.write.saveAsTable("{0}_pg".format(self.table))
        self.df.registerTempTable("{0}_temp".format(self.table))
        spark.sql("CREATE TABLE {0}_pg AS SELECT * FROM {0}_temp".format(self.table))

    def run(self):
        self.read()
        self.write()
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


##################################################################
# Class that executes postgresql query and saves to HDFS CSV files
##################################################################
class QueryToHDFS(Query):
    query = luigi.Parameter()
    path = luigi.Parameter()
    delimiter = luigi.Parameter(default='|')

    def write(self):
        self.df.write.option("header","true").option("delimiter",self.delimiter).csv(self.path)




if __name__ == '__main__':
    cmdline_args = sys.argv[1:]
    # if there was a unique identifier passed we remove from the arguments and pass separately
    unique_id = None
    if '--unique-identifier' in cmdline_args:
        index = cmdline_args.index('--unique-identifier')
        flag = cmdline_args.pop(index)
        unique_id = cmdline_args.pop(index)

    build_and_run(cmdline_args, unique_id=unique_id)