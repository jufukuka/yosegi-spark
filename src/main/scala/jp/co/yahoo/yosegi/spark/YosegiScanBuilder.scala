package jp.co.yahoo.yosegi.spark

import jp.co.yahoo.yosegi.config.Configuration
import jp.co.yahoo.yosegi.spark.utils.ProjectionPushdownUtil
import jp.co.yahoo.yosegi.spread.expression.AndExpressionNode

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownAggregates}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

case class YosegiScanBuilder(
                            sparkSession: SparkSession,
                            fileIndex: PartitioningAwareFileIndex,
                            schema: StructType,
                            dataSchema: StructType,
                            options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  override def build(): Scan = {
    lazy val hadoopConf = {
      val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
      sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    }
    YosegiScan(
      sparkSession,
      hadoopConf,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      options,
      pushedDataFilters,
      partitionFilters,
      dataFilters
    )
  }
}
