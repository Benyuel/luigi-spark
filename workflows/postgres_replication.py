import os.path
import sys
sys.path.insert(0, os.path.abspath(".."))
import luigi
import psycopg2
from pipeline import SparkSubmitTask

repo = luigi.configuration.get_config().get('repo','dir')
deploy_mode = luigi.configuration.get_config().get('spark','deploy-mode')
queue = luigi.configuration.get_config().get('spark','queue')

class PostgresSource():
    def __init__(self):
        self.host = luigi.configuration.get_config().get('postgres', 'host')
        self.database = luigi.configuration.get_config().get('postgres', 'database')
        self.user = luigi.configuration.get_config().get('postgres', 'user')
        self.password = luigi.configuration.get_config().get('postgres', 'password')
        self.port = luigi.configuration.get_config().get('postgres', 'port')
        self.connection = self.connect()
        self.cursor = self.connection.cursor()

    def connect(self):
        connection = psycopg2.connect(
          host=self.host,
          port=self.port,
          database=self.database,
          user=self.user,
          password=self.password)

        connection.set_client_encoding('utf-8')
        connection.autocommit = True
        return connection

    def query(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()


class Run(luigi.WrapperTask):
    INCREMENTAL_SIZE_THRESHOLD = luigi.IntParameter(default=50000)
    hive_db = luigi.Parameter(luigi.configuration.get_config().get('hive','db'))

    @property
    def table_metadata(self):
        sql = '''
          WITH table_has_id_column AS (
            SELECT 
              relname AS tablename,
              TRUE AS table_has_id_column
            FROM pg_class
            LEFT JOIN pg_attribute ON pg_attribute.attrelid = pg_class.relname::regclass
            WHERE attname = 'id' AND relkind = 'r' AND (relhasindex OR NOT SUBSTRING(relname FOR 4) = 'sql_')
          )
          , table_has_updated_at_column AS (
            SELECT 
              relname AS tablename,
              TRUE AS table_has_updated_at_column
            FROM pg_class
            LEFT JOIN pg_attribute ON pg_attribute.attrelid = pg_class.relname::regclass
            WHERE attname = 'updated_at' AND relkind = 'r' AND (relhasindex OR NOT SUBSTRING(relname FOR 4) = 'sql_')
          )
          , table_has_created_at_column AS (
            SELECT 
              relname AS tablename,
              TRUE AS table_has_created_at_column
            FROM pg_class
            LEFT JOIN pg_attribute ON pg_attribute.attrelid = pg_class.relname::regclass
            WHERE attname = 'created_at' AND relkind = 'r' AND (relhasindex OR NOT SUBSTRING(relname FOR 4) = 'sql_')
          )
          , table_official_pkey AS (
            SELECT 
              relname AS tablename,
              attname AS official_pkey
            FROM pg_class t
            LEFT JOIN pg_index i ON i.indrelid = t.relname::regclass
            LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indisprimary AND relkind = 'r' AND relhasindex
          )

          SELECT 
            relname AS tablename,
            relpages AS tablesize,
            reltuples AS numrows,
            relnatts AS numcols,
            CASE WHEN table_has_id_column IS NULL THEN FALSE ELSE table_has_id_column END,
            CASE WHEN table_has_updated_at_column IS NULL THEN FALSE ELSE table_has_updated_at_column END,
            CASE WHEN table_has_created_at_column IS NULL THEN FALSE ELSE table_has_created_at_column END,
            STRING_AGG(official_pkey, ',') AS official_pkeys
              
          FROM pg_class t
          LEFT JOIN table_has_id_column i ON i.tablename = t.relname
          LEFT JOIN table_has_updated_at_column u ON u.tablename = t.relname
          LEFT JOIN table_has_created_at_column c ON c.tablename = t.relname
          LEFT JOIN table_official_pkey p ON p.tablename = t.relname
          WHERE t.relkind = 'r' AND NOT (relname LIKE 'sql_%') AND NOT (relname LIKE 'pg_%') 
          GROUP BY 1,2,3,4,5,6,7
        '''

        client = PostgresSource()
        res = client.query(sql)

        table_metadata = {}
        for row in res:
            table_metadata[row[0]] = {
              'size': row[1],
              'rows': row[2],
              'cols': row[3],
              'has_id': row[4],
              'has_updated_at': row[5],
              'has_created_at': row[6],
              'primary_key': row[7]
            }

        return table_metadata


    def requires(self):
        table_metadata = self.table_metadata

        for table in table_metadata.keys():
            primary_key_arg = ""
            timestamp_field_arg = ""
            incremental_arg = ""

            if table_metadata[table]['size'] >= self.INCREMENTAL_SIZE_THRESHOLD:
                if not table_metadata[table]['primary_key'] or (
                    not table_metadata[table]['has_created_at'] 
                    and not table_metadata[table]['has_updated_at']
                    ):
                    pass #we will do this not incremental
                else:
                    primary_key_arg = "--primary-key {0}".format(table_metadata[table]['primary_key'])
                    incremental_arg = "--incremental"
                    if table_metadata[table]['has_updated_at']:
                        timestamp_field_arg = "--timestamp-field updated_at"
                    else:
                        timestamp_field_arg = "--timestamp-field created_at"

            yield SparkSubmitTask(app='{0}/postgres.py'.format(repo),
                                  entry_class='Table',
                                  app_options='''--table {0} --hive-db {1} {2} {3} {4} --dest-suffix {5} --jdbc-ssl'''.format(table, self.hive_db, primary_key_arg, timestamp_field_arg, incremental_arg, "_pg"),
                                  deploy_mode=deploy_mode,
                                  queue=queue,
                                  packages='org.postgresql:postgresql:9.4.1211',
                                  local=False,
                                  executor_memory='40g')




if __name__ == '__main__':
    luigi.run()