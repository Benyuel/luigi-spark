
#### Notes 

* on hadoop stack, it's important to set `max_reschedules>=2` in your `luigi.cfg`

* Adding permissions to the database
```
USE default_db;
CREATE ROLE me;
GRANT ALL ON DATABASE default_db TO ROLE me;
GRANT ROLE me TO GROUP my_ad_group;
```

* To get the size of tables on hadoop do
```javascript
hadoop fs -du /user/dir | awk '/^[0-9]+/ { print int($1/(1024**3)) " [GB]\t" $2 }'
```

* kill all accepted spark jobs
```
for app in `yarn application -list | awk '$6 == "ACCEPTED" { print $1 }'`; do yarn application -kill "$app";  done
```

* You can change the replication factor of a directory using command:
```
hdfs dfs -setrep -R 2 /user/hdfs/test
```

* But changing the replication factor for a directory will only affect the existing files and the new files under the directory will get created with the default replication factor (dfs.replication from hdfs-site.xml) of the cluster.

* You can see the replication factor by doing `hdfs dfs -ls` where the replication factor is shown before the user column
* But you can temporarily override and turn off the HDFS default replication factor by passing: `-D dfs.replication=1`

* Prior to doing any altering or creating tables, etc., do `USE default_db;`
* JOINs are expensive in SparkSQL/HiveQL
* 3 Types of SQL Execution Engines available depending on the job/query (`Hive`, `Tez`, `MR`, `Spark`)
* `JSON_EXTRACT_PATH_TEXT(field, 'key')` -> `GET_JSON_OBJECT(field, '$.key')`
* Indexing/Partitioning on a dt field may help query performance
* in hive, it is better to do the filter in subqueries before the join
* Some `hive`/`tez`/`mr` engine params useful for tuning:
```
SET hive.exec.orc.split.strategy=BI;
SET mapreduce.map.memory.mb=6192;
SET hive.tez.java.opts=-Xmx6192M;
SET hive.tez.container.size=6192;
```

* you can't typically do `ALTER TABLE DROP COLUMN` in older versions of hive/sparksql.  Instead do the following where `column_X` is the column(s) you want to keep:

```
ALTER TABLE table REPLACE COLUMNS (column_X STRING, ....)
```

* to see space quote info on hdfs do `hadoop fs -count -q /user/dir`

* to see table sizes do `hadoop fs -du /user/dir/default_hiveDB.db/ | awk '/^[0-9]+/ { print int($1/(1024**3)) " [GB]\t" $2 }'`

* to check hdfs status of a dir do `hadoop fsck /user/dir/`

* For a spark python env to use custom python modules/files/requirements or a different python version, below is an example:

```
conda create -n py3spark_env --copy -y -q python=3
source activate py3spark_env
pip install shapely numpy pandas luigi gspread>=0.2.5 oauth2client==1.5.2 slackclient>=0.16
# or
conda install pyopenssl
source deactivate py3spark_env

cd ~/anaconda3/envs/
zip -r py3spark_env.zip py3spark_env

export SPARK_HOME=/spark-2.0.1
export PYSPARK_PYTHON=./py3spark_env/bin/python3
/spark-2.0.1/bin/pyspark --master yarn --deploy-mode client --queue public --executor-memory 30g --driver-memory 5g --archives py3spark_env.zip
```

* Some `spark` configs useful for tuning:

```
        .config("spark.sql.crossJoin.enabled","true") \
        .config("spark.sql.hive.verifyPartitionPath","false") \
        .config("hive.exec.dynamic.partition.mode","nonstrict") \
        .config("hive.exec.dynamic.partition", "true") \
```

* In spark, if you want to save a DataFrame as a hive table, use:

```
df.registerTempTable("table_temp")
spark.sql("create table table_x AS SELECT * FROM table_temp")
```

* Doing `df.write.format(format_x).saveAsTable...` causes problems when querying via beeline


* determining deploy-mode programatically
```
if spark.conf.get("spark.submit.deployMode") == 'client':
    config = read_local_config()
elif spark.conf.get("spark.submit.deployMode") == 'cluster':
    config = read_hdfs_config(spark)
```