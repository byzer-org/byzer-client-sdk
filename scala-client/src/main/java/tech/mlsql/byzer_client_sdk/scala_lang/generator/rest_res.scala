package tech.mlsql.byzer_client_sdk.scala_lang.generator

case class ResSchemaFiled(name: String)

case class ResSchema(`type`: String, fields: List[ResSchemaFiled])

case class Res[T](schema: ResSchema, data: List[T])(implicit m: Manifest[T])

case class ResTableSchema(col_name: String, data_type: String)


case class ResShufflePerf(
                           memoryBytesSpilled: Long,
                           diskBytesSpilled: Long,
                           inputRecords: Long

                         )

case class ResResource(
                        currentJobGroupActiveTasks: Int,
                        activeTasks: Int,
                        failedTasks: Int,
                        completedTasks: Int,
                        totalTasks: Int,
                        taskTime: Double,
                        gcTime: Double,
                        activeExecutorNum: Int,
                        totalExecutorNum: Int,
                        totalCores: Int,
                        usedMemory: Double,
                        totalMemory: Double,
                        shuffleData: ResShufflePerf
                      )

case class ResJobProgress(var totalJob: Long = 0, var currentJobIndex: Long = 0, var script: String = "")

case class ResJobInfo(
                       owner: String,
                       jobType: String,
                       jobName: String,
                       jobContent: String,
                       groupId: String,
                       progress: ResJobProgress,
                       startTime: Long,
                       timeout: Long
                     )

case class ResJobDetail(
                         val jobId: Int,
                         val submissionTime: Option[java.sql.Date],
                         val completionTime: Option[java.sql.Date],
                         val numTasks: Int,
                         val numActiveTasks: Int,
                         val numCompletedTasks: Int,
                         val numSkippedTasks: Int,
                         val numFailedTasks: Int,
                         val numKilledTasks: Int,
                         val numCompletedIndices: Int,
                         val numActiveStages: Int,
                         val numCompletedStages: Int,
                         val numSkippedStages: Int,
                         val numFailedStages: Int,
                         val duration: Long
                       )

case class ResVersion(version: String,
                      buildBy: String,
                      date: String,
                      srcChecksum: String,
                      revision: String,
                      branch: String,
                      url: String,
                      core: String
                     )

case class ResDataSource(name: String)

case class ResDataSourceParam(param: String, description: String, value: String, extra: String)

case class ResET(name: String, algType: String, sparkCompatibility: String, doc: String, docType: String)

case class ResETParam(param: String, description: String, value: String, extra: String)

case class ResTable(database: String, tableName: String, isTemporary: Boolean)