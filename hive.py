import logging
import sys
from urllib.parse import urlunsplit
import subprocess
import luigi
from luigi.contrib.simulate import RunAnywayTarget
# to handle importing via --py-files vs package management
try:
    from interface import build_and_run, read_hdfs_config, DEFAULT_WAREHOUSE_DIR
    from hdfs_ import Directory
except:
    from spark_utils.interface import build_and_run, read_hdfs_config, DEFAULT_WAREHOUSE_DIR
    from spark_utils.hdfs_ import Directory
# to handle within sparkContext or not
try:
    from pyspark.sql import SparkSession,HiveContext
except:
    print('could not import pyspark...continuing anyway...')

logger = logging.getLogger('luigi-interface')

###############################################################
# sparkSQL methods for interacting with hive 
# requires param sparkSession
###############################################################
def exists(table, db, spark):
    spark.sql("USE {0}".format(db))
    sql = "SHOW TABLES LIKE '{0}'".format(table)
    results = spark.sql(sql)
    return (results.count() > 0) #is there a single result

def drop(table, db, spark, purge=False, fs=True):
    spark.sql("USE {0}".format(db))
    sql = "DROP TABLE IF EXISTS {0}".format(table)
    # option only works in spark 2.1+
    if purge:
        sql += ' PURGE'
    spark.sql(sql).collect()
    if fs:
        # database name must be like *_db
        table_dir = DEFAULT_WAREHOUSE_DIR + db.replace('db','hiveDB.db') + '/' + table
        try:
            status = Directory(table_dir).delete(recursive=True)
        except Exception as e:
            print(e)

def latest_timestamp(table, db, timestamp_field, spark):
    # make sure there are records in the table
    sql = """SELECT * FROM {0}.{1} LIMIT 10""".format(db, table)
    assert spark.sql(sql).count() > 0, 'table {0}.{1} is empty...'.format(db, table)
    # pull latest timestamp field and return string
    sql = """SELECT MAX( CAST({0} AS TIMESTAMP) ) as {0} FROM {1}.{2} WHERE {0} IS NOT NULL""".format(timestamp_field, db, table)
    latest = spark.sql(sql)
    return latest.head(1)[0][timestamp_field].strftime('%Y-%m-%d %H:%M:%S')

def dedupe(base_table, temp_table, reconcile_table, db, timestamp_field, primary_key, spark):
    spark.sql("USE {0}".format(db))
    drop(reconcile_table, db, spark)
    spark.sql("""CREATE TABLE {0} AS
             SELECT t1.* FROM
             (SELECT * FROM {1}
                 UNION ALL
              SELECT * FROM {2}) t1
             JOIN
                 (SELECT {3}, max( CAST({4} AS TIMESTAMP) ) {4} FROM
                     (SELECT * FROM {1}
                        UNION ALL
                      SELECT * FROM {2}) t2
                 GROUP BY t2.{3}) s
             ON s.{3} = t1.{3}
            AND s.{4} = t1.{4}""".format(reconcile_table,
                                         base_table,
                                         temp_table,
                                         ", ".join(primary_key.split(",")),
                                         timestamp_field))
    drop(base_table, db, spark)
    # spark.sql("CREATE TABLE {0} LIKE {1}".format(base_table, reconcile_table))
    # spark.sql("INSERT INTO {0} SELECT * FROM {1}".format(base_table, reconcile_table))
    spark.sql("ALTER TABLE {0} RENAME TO {1}".format(reconcile_table, base_table))
    drop(temp_table, db, spark)
    drop(reconcile_table, db, spark)

def add_column(table, db, column, datatype, spark):
    spark.sql("USE {0}".format(db))
    spark.sql("ALTER TABLE {0} ADD COLUMNS ({1} {2})".format(table, column, datatype))

###############################################################
# SparkSQL job to submitted using pipeline.py
###############################################################
class SparkSQLTask(luigi.ExternalTask):
    sql = luigi.Parameter()
    context = luigi.Parameter(default='spark') # or hive

    @property
    def spark(self):
        return SparkSession \
          .builder \
          .appName("Luigi on Spark - SQL") \
          .config("hive.metastore.warehouse.dir", DEFAULT_WAREHOUSE_DIR) \
          .config("spark.driver.extraClassPath", "/usr/hdp/current/hadoop-client/lib/hadoop-lzo-0.6.0.2.2.0.0-2041.jar") \
          .config("spark.driver.extraLibraryPath", "/usr/hdp/current/hadoop-client/lib/native:/usr/hdp/current/hadoop-client/lib/native/Linux-amd64-64") \
          .config("spark.yarn.submit.waitAppCompletion", "true") \
          .enableHiveSupport() \
          .getOrCreate()

    @property
    def hive(self):
        return HiveContext(self.spark._sc)

    def valid(self, sql):
        # check to make sure it's valid sql
        if bool(sql.strip()):
            if '\n' not in sql and sql.strip().startswith("--"):
                return False
            else:
                return True
        else:
            return False

    def run(self):
        if self.context == 'spark':
            context = self.spark
        elif self.context == 'hive':
            context = self.hive
        else:
            raise NotImplementedError
        # can only execute 1 statement at a time in SparkSQL
        for sql in self.sql.split(';'):
            if self.valid(sql):
                context.sql(sql)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


###############################################################
# Hive query task that does not need a sparkContext 
###############################################################
class HiveQueryTask(luigi.Task):
    """ Runs a hive query.

    A helpful task that encapsulates some useful logic when running a hive
    query and captures the necessary parameters to do so. Provides a helper
    method `execute` to run the hive command specified that returns a
    subprocess.CompletedProcess:
    https://docs.python.org/3/library/subprocess.html#subprocess.CompletedProcess

    By default, execute opens up stdout and stderr so the CompletedProcess
    will contain stdout and stderr.

    By default, execution will error if the hive command exists with a non-zero
    exit code.
    """
    hive_cli          = luigi.Parameter(default='beeline')

    hive_driver       = luigi.Parameter(default='jdbc:hive2')
    hive_server       = luigi.Parameter()
    hive_port         = luigi.Parameter()
    hive_database     = luigi.Parameter()

    hive_username     = luigi.Parameter(significant=False)
    hive_password     = luigi.Parameter(significant=False)

    @property
    def hive_query(self):
        raise NotImplementedError()

    @property
    def hive_args(self):
        """ Additional hive arguments if necessary."""
        return []

    @property
    def hive_base_command(self):
        server_path = self.hive_server + ':' + self.hive_port
        url_parts = (self.hive_driver, server_path, self.hive_database, '', '')
        connection_string = urlunsplit(url_parts)

        base_hive_command = [
            self.hive_cli,
            '-u', connection_string,
            '-n', self.hive_username,
            '-p', self.hive_password]

        return base_hive_command

    @property
    def hive_command(self):
        if not self.hive_query:
            raise ValueError('A hive query needs to be provided to be run.')

        hive_command = (
            self.hive_base_command +
            ['-e', self.hive_query] +
            self.hive_args)

        return hive_command

    def run(self):
        logger.debug('Executing query: {}'.format(self.hive_query))
        logger.debug('Additional query args: {}'.format(self.hive_args))

        result = subprocess.run(
            self.hive_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True)
        # TODO: Figure out a way to check for error
        return result




if __name__ == '__main__':
    cmdline_args = sys.argv[1:]
    # if there was a unique identifier passed we remove from the arguments and pass separately
    unique_id = None
    if '--unique-identifier' in cmdline_args:
        index = cmdline_args.index('--unique-identifier')
        flag = cmdline_args.pop(index)
        unique_id = cmdline_args.pop(index)

    build_and_run(cmdline_args, unique_id=unique_id)
