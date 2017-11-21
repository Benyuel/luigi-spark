

Self-contained layered data pipelines within Spark and python using luigi

Using `pipeline.py` to submit your spark app, you can have traditional luigi task trees run all within a pyspark context and handle resulting task statuses in yarn logs.  
 
#### Install

###### Simple

`bin/install`

###### Manual

* Tested against Hortonworks Hadoop 2.6.0, Python 3.5, Spark 2.0.1, CentOS

```
Hadoop 2.6.0.2.2.0.0-2041
Subversion git@github.com:hortonworks/hadoop.git -r 7d56f02902b436d46efba030651a2fbe7c1cf1e9
```

* requires standard-lib python2 on Linux for install

```
cd ~/
git clone this-repo
```

* fill out your `luigi.cfg`.  Example in this repo

```
python setup.py install
```

* follow the command prompts to accept the license and default install path

* answer yes to `Do you wish the installer to prepend the Anaconda3 install location to PATH in your /home/user.../.bashrc ?`

* To place your `luigi.cfg` into production use on hdfs, do

```
hadoop fs -put $DEFAULT_LUIGI_CONFIG_PATH_LOCAL $DEFAULT_LUIGI_CONFIG_PATH_HDFS
```

* start the luigi deamon task monitor

```
luigid --background --pidfile ../pidfile --logdir ../logs --state-path ../statefile
```

* then monitor your tasks on `host:8082`

* to install this repo as a package and overwrite an existing installation do the following (needs to be run when deploying updates to this repo)

```
python setup.py install
```

#### Executables

* `bin/beeline`: connects to hive server via beeline to run interactive queries
* `bin/install`: installs this package and sets everything up
* `bin/luigid`: starts luigi daemon process
* `bin/prod`: sets up this repo ready for production usage
* `bin/pyenv`: recreates the required python env
* `bin/pyspark`: starts the pyspark shell for interactive usage/debugging
* `bin/pysparksql`: executes arbitary sql using spark or hive contexts (no results returned)
* `bin/workflow`: executes the user input'ed workflow/task
* `bin/kill`: kills apps using -s SEARCHSTRING -t STATE -q QUEUE
* `bin/kill_app_after_x_min`: kills all apps running for more than x minutes
* `bin/search_log`: searches yarn logs for the given search string and returns the given logtype
* `bin/search_app`: searches running apps for the given search string and returns the app's status

#### Running Workflows

* Example:

```
source ~/.bashrc
export LUIGI_CONFIG_PATH=~/this-repo/luigi.cfg
cd this-repo/workflows
python3 -m luigi --module postgres_replication Run --workers 10
```


#### Running Tasks

* Kicking off jobs can be scheduled / managed using `clock.py`, `/workflows`, or via a shell script.  We typically use `/workflows` for our production task workflows

###### `/workflows`

* Does something to determine dependencies and then kicks off the necessary `pipeline.py SparkSubmitTask`

* Example: `workflows/postgres_replication.py`
  * Connects to Postgres db, determines based on the size which tables will be incremental or not, then launches the `postgres.py Table` spark job via `pipeline.py SparkSubmitTask` for each table required on the import

###### `clock.py` (also see `schedule.py`)

* pure python alternative to cron

* run `python clock.py` to start all jobs defined in `clock.py`

* (screen)[https://www.gnu.org/software/screen/manual/screen.html] can be used to manage the `clock.py` application connection (`Contrl C` will insert you into the running clock.py process where you can dynamically schedule or clear jobs)

```
# list running screen sessions
screen -r
# list all screen sessions
screen -ls
# kill screen session 10031.pts-50.host1
screen -S 10031.pts-50.host1 -p 0 -X quit
# connect to existing screen session 10031.pts-50.host1
screen -d -r 10031.pts-50.host1
```


###### Running DDL SparkSQL

```
cd ~/this-repo/workflows/
export LUIGI_CONFIG_PATH=../luigi.cfg
python spark_sql.py test_master
```

#### Performance
* Testing performance of a job can be done using sparklint and submitting the spark job with:
```
--conf spark.extraListeners=com.groupon.sparklint.SparklintListener 
--packages com.groupon.sparklint:sparklint-spark201_2.11:1.0.4
```
* then open a browser and navigate to your driver node’s port 23763


#### Contributing
* NOTE: Deploying any major change requires recreating the PythonENV so that spark-utils are available within any given spark-context

```
python pipeline.py PythonENV
```

* Tests go in `tests/`
* To execute a test file, run `python module_test.py`
* Handle your test paths/imports using `context.py`


###### Example Layering - All layers connected to the Central Scheduler

* Luigi Task Layer 1 - `pipeline.py` submits the spark job
* Luigi Task Layer 2 - luigi runs the `postgres.py Table` task within Spark cluster and reports completion time, errors, status
* Luigi Task Layer 1 - 'pipeline.py' checks the yarn logs to make sure the task succeeded


#### Class Descriptions
* setup.py
 * `class Error` : handling general exceptions in this module
 * `class Python` : by default, install Anaconda python 3.5.2
* interface.py 
 * `read_*_config` : reads local or hdfs config
 * `port_is_open` : check if scheduler is running
 * `get_task` : cmdline parser to luigi.Task
 * `build` : build luigi workflow programmatically
 * `run` : thread safe alternative to `build`
 * `build_and_run` : wraps `get_task` and `run`
 * class decorators for custom logic to handle task statuses 
* scheduler.py
 * `Scheduler` : python implementation of cron alternative 
 * `SafeScheduler` : scheduler loop will not fail if a job fails
 * `Job` : handles the actual execution of a job
* google_sheet.py
 * `class Sheet` : Writes all contents of one tab from google sheet to a hive table
 * `class SheetToHDFS` : Writes all contents of one tab from google sheet to hdfs path folder
* hdfs.py
 * `class Directory` : hdfs directory utility 
 * `class File` : hdfs file utility (i.e. create/delete/read/write/describe)
* pipeline.py
 * `class PythonENV` : creates a spark ready python env
  * inputs: 
   * `packages`: list of python packages to be installed by pip
   * `packages_conda`: list of pyhton packages to be installed by conda
   * `python_version`: i.e. 3.5
   * `env_name` : name to call this env that is created (default = `py3spark_env`)
   * `python_install` : if `true`, then the class `PythonInstall` is ran before creating the env
  * output: resulting env.zip will be in `~/anaconda3/envs/` directory
 * `class SparkSubmitTask`: given inputs and spark parameters and job, attemps to submit it for you
  * an abstraction of `spark-submit`
* postgres.py
 * `class Table` : replicates a postgres table to a hive table (incrementally or from scratch)
 * `class Query` : creates a hive table based on results of postgres Query
 * `class QueryToHDFS` : replicates a postgres table to hdfs folder path
* hive.py:
 * `HiveQueryTask` : Uses beeline to run a query
