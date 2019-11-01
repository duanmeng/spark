
## Update by Tencent
[TASK-6742589][bug-51766279] fix OutOfMemoryError for DAGScheduler's task serialization
[TASK-6742603][story-58141649][CORE] support tdw-hdfs HA
[TASK-6742603][CORE] Redact tdw related properties
[TASK-6778333][story-58141937] add system property "tdw.ugi.groupname" for doctor
[TASK-6778335][story-58142055] hide user defined app name for lz/tesla
[TASK-6778389][minor] set 'Xms' equal to 'Xmx'
[TASK-6789919][minor] log spark.hadoop.hadoop.job.ugi for issues checking
[TASK-6789773][story-57349489] support "show row count" for rcfile/orcfile table
[TASK-6789531][story-57285327] add all python libs to PYTHONENV when submitted by yarn-client mode for pyspark
[TASK-6789487][story-57190985] auto distribute all python/lib/*.zip
[TASK-6789457][story-58345755] set "spark.sql.warehouse.dir" before am start running
[TASK-6789255][story-58460455] add GenericOptionsParser for parsing "-D ***"
[TASK-6788991][story-58471633] change eventlog to 755
[TASK-6788895][minor] package mysql and postgresql driver
[TASK-6788801][story-58349555] switch time zone from GMT to UTC
[TASK-6788401][story-59641267] expose Broadcast.destroy(blocking)
[TASK-6790741][story-58103269] upload log4j-cluster.properties special for yarn-cluster
[TASK-6790649][story-57347843] change permission of staging dir to 755
[TASK-6790553][story-57338453] limit am and executor memory separately to avoid yarn's memory exhausted
[TASK-6794133]FIX charset coding exception
[TASK-6793881]compatible with mr 1.0 & 2.0
[TASK-6793761]compatible with tesla dynamic resource schedule(the default value of executor num is set as 0)
[TASK-6793383]recovery ip as host not FQDN. ref SPARK-21642
[TASK-6793345]FIX py4j version
[TASK-6793071][story-57163177] add TeslaMonitorListener to StreamingListenerBus
[TASK-6792747]support showing tdw-hdfs path on ui
[TASK-6792279][story-56924671] provide sortMergeGroupByKey to support skewed group by
[TASK-6791523][minor] limit event log size to avoid replaying slowly
[TASK-6791431][story-58242481] add spark.yarn.client.uploadHadoopConf.enabled to decide whether upload hadoop conf
[TASK-6797995]update minimum pandas version to 0.17.1
[TASK-6797927]compatible with 1.3.0 lz4
[TASK-6797707]compatible tdw hadoop ugi
[TASK-6796699][story-60032821] support idex job location
[TASK-6796289][story-59651909] limit the size of stdout and stderr
[TASK-6795873][story-58093823] just display the root log url for yarn mode
[TASK-6795735]fix staging dir can not be deleted
[TASK-6742035]Compatible Hadoop 2.2.0
[TASK-6778365][story-57178259] create a graph from VertexRDD and ReplicatedVertexView
[TASK-6778357][story-58101959] support pyspark import .so
[TASK-6805495][story-57229795] support python roc metrics
[STORY-856017483][CORE] Add checking number of transferTo zero returns to avoid infinite loop in some occasional cases
[SuperSQL] 更新SuperSql Connector对应的JDBC URL前缀，由jdbc:supersql改为jdbc:supersql:datasource
[SuperSQL]支持Spark作为执行引擎
[SuperSQL] 通过Session参数修改SuperSQL Server Lex属性，确保下推SQL标识符加双引号符合语法
[SuperSQL] 优化参数透传Jdbc数据源的逻辑，resolveTable之前也加上参数透传

## AE related
set the uncaughtExceptionHandler in parent thread to catch the exception of child thread (#91)
fix the zero-sized blocks should be excluded exception
Change output partitioning of ShuffleQueryStageInput when local shuffle
Disable change reduce number if the joins are changed
Change the synchronized to lock in wait for subquery to resolve the hang issue
Avoid the prepareExecuteStage#QueryStage method is executed multi-times when call executeCollect, executeToIterator and executeTake action multi-times
disable auto calculate the reduce number when the pre-shuffle partition number is not same
Support Left Anti Join in data skew feature
Support left/right outer join in handling data skew feature
fix AE job desc
Add information about external shuffle service in readme
Update README.md
Equally divide the mapper ids in a skewed partition
Handle skewed join at runtime
Collect row counts at runtime
In BHJ, shuffle read should be local always
Fix exception: Child of ShuffleQueryStage must be a ShuffleExchange
Optimize SortMergeJoin to BroadcastHashJoin at runtime
Change the private variable name from stats to partitionStats in InMemoryTableScanExec to avoid the private value override the stats method in SparkPlanStats
Enable stats estimation for physical plan
Allow shuffle readers to request data from just one mapper
set correct execution Id for broadcast query stage
Add QueryStage and the framework for adaptive execution
Optimize shuffle fetch of contiguous partition

## Backport from Apache Spark
[SPARK-28977][DOCS][SQL] Fix DataFrameReader.json docs to doc that partition column can be numeric, date or timestamp type
[SPARK-28921][K8S][FOLLOWUP] Also bump K8S client version in integration-tests
[SPARK-28709][DSTREAMS] Fix StreamingContext leak through Streaming
[SPARK-22955][DSTREAMS] - graceful shutdown shouldn't lead to job gen…
[SPARK-29042][CORE][BRANCH-2.4] Sampling-based RDD with unordered input should be INDETERMINATE
[MINOR][SS][DOCS] Adapt multiple watermark policy comment to the reality
[SPARK-29124][CORE] Use MurmurHash3 `bytesHash(data, seed)` instead of `bytesHash(data)`
[SPARK-29104][CORE][TESTS] Fix PipedRDDSuite to use `eventually` to check thread termination
[SPARK-26713][CORE][2.4] Interrupt pipe IO threads in PipedRDD when task is finished
[SPARK-29046][SQL][2.4] Fix NPE in SQLConf.get when active SparkContext is stopping
[SPARK-25277][YARN] YARN applicationMaster metrics should not register static metrics
[SPARK-29087][CORE][STREAMING] Use DelegatingServletContextHandler to avoid CCE
[SPARK-27122][CORE][2.4] Jetty classes must not be return via getters in org.apache.spark.ui.WebUI
[SPARK-26989][CORE][TEST][2.4] DAGSchedulerSuite: ensure listeners are fully processed before checking recorded values
[SPARK-29079][INFRA] Enable GitHub Action on PR
[SPARK-29045][SQL][TESTS] Drop table to avoid test failure in SQLMetricsSuite
[SPARK-24663][STREAMING][TESTS] StreamingContextSuite: Wait until slow receiver has been initialized, but with hard timeout
[SPARK-29075][BUILD] Add enforcer rule to ban duplicated pom dependency
[SPARK-29073][INFRA][2.4] Add GitHub Action to branch-2.4 for `Scala-2.11/2.12` build
[MINOR][DOCS] Fix few typos in the java docs
[SPARK-28906][BUILD] Fix incorrect information in bin/spark-submit --version
[SPARK-23519][SQL][2.4] Create view should work from query with duplicate output columns
[SPARK-29101][SQL][2.4] Fix count API for csv file when DROPMALFORMED mode is selected
[SPARK-28340][CORE] Noisy exceptions when tasks are killed: "DiskBloc…
[SPARK-29003][CORE] Add `start` method to ApplicationHistoryProvider to avoid deadlock on startup
[SPARK-19147][CORE] Gracefully handle error in task after executor is stopped
[SPARK-27460][TESTS][2.4] Running slowest test suites in their own forked JVMs for higher parallelism
[SPARK-26003][SQL][2.4] Improve SQLAppStatusListener.aggregateMetrics performance
[SPARK-26435][SQL] Support creating partitioned table using Hive CTAS by specifying partition column names
[SPARK-20774][SPARK-27036][SQL] Cancel the running broadcast execution on BroadcastTimeout
[SPARK-28662][SQL] Create Hive Partitioned Table DDL should fail when partition column type missed
[SPARK-27243][SQL] RuleExecutor.dumpTimeSpent should not throw exception when empty
[SPARK-26892][CORE] Fix saveAsTextFile throws NullPointerException when null row present
[SPARK-26794][SQL] SparkSession enableHiveSupport does not point to hive but in-memory while the SparkContext exists
[SPARK-26224][SQL][PYTHON][R][FOLLOW-UP] Add notes about many projects in withColumn at SparkR and PySpark as well
[SPARK-26224][SQL] Advice the user when creating many project on subsequent calls to withColumn
[SPARK-25680][SQL] SQL execution listener shouldn't happen on execution thread
[SPARK-25158][SQL] Executor accidentally exit because ScriptTransformationWriterThread throw Exception.
[SPARK-29053][WEBUI][2.4] Sort does not work on some columns
[SPARK-27073][CORE] Fix a race condition when handling of IdleStateEvent
[SPARK-29112][YARN] Expose more details when ApplicationMaster reporter faces a fatal exception
[SPARK-27676][SQL][SS] InMemoryFileIndex should respect spark.sql.files.ignoreMissingFiles
[SPARK-27801][SQL] Improve performance of InMemoryFileIndex.listLeafFiles for HDFS directories with many files
[SPARK-27202][MINOR][SQL] Update comments to keep according with code
[SPARK-25062][SQL] Clean up BlockLocations in InMemoryFileIndex
[SPARK-27253][SQL][FOLLOW-UP] Add a legacy flag to restore old session init behavior
[SPARK-27253][SQL][FOLLOW-UP] Update doc about parent-session configuration priority
[SPARK-27253][SQL][FOLLOW-UP] Add a note about parent-session configuration priority in migration guide
[SPARK-27253][SQL] Prioritizes parent session's SQLConf over SparkConf when cloning a session
[SPARK-28599][SQL][2.4] Fix `Duration` column sorting for ThriftServerSessionPage
[SPARK-25903][CORE] TimerTask should be synchronized on ContextBarrierState
[SPARK-29203][SQL][TESTS][2.4] Reduce shuffle partitions in SQLQueryTestSuite
[SPARK-23197][STREAMING][TESTS][2.4] Fix ReceiverSuite."receiver_life_cycle" to not rely on timing
[SPARK-29229][SQL] Change the additional remote repository in IsolatedClientLoader to google minor
[SPARK-28678][DOC] Specify that array indices start at 1 for function slice in R Scala Python
[SPARK-28938][K8S][2.4] Move to supported OpenJDK docker image for Kubernetes
[SPARK-25753][CORE][2.4] Fix reading small files via BinaryFileRDD
[SPARK-29286][PYTHON][TESTS] Uses UTF-8 with 'replace' on errors at Python testing script
[SPARK-29203][TESTS][MINOR][FOLLOW UP] Add access modifier for sparkConf in SQLQueryTestSuite
[SPARK-29244][CORE][FOLLOWUP] Fix java lint error due to line length
[SPARK-29244][CORE][FOLLOWUP] Fix compilation
[SPARK-29244][CORE] Prevent freed page in BytesToBytesMap free again
[SPARK-29055][CORE] Update driver/executors' storage memory when block is removed from BlockManager
[SPARK-29186][SQL][2.4][FOLLOWUP] AliasIdentifier should be converted to Json in prettyJson
[SPARK-29186][SQL] AliasIdentifier should be converted to Json in prettyJson
[SPARK-29247][SQL] Redact sensitive information in when construct HiveClientHive.state
[SPARK-29263][CORE][TEST][FOLLOWUP][2.4] Fix build failure of `TaskSchedulerImplSuite`
[SPARK-29263][SCHEDULER] Update `availableSlots` in `resourceOffers()` before checking available slots for barrier taskSet
[SPARK-29240][PYTHON] Pass Py4J column instance to support PySpark column in element_at function
[SPARK-29213][SQL] Generate extra IsNotNull predicate in FilterExec
[SPARK-27051][CORE] Bump Jackson version to 2.9.8
[SPARK-24601] Update Jackson to 2.9.6