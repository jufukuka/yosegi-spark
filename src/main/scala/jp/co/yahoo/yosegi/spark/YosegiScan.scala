package jp.co.yahoo.yosegi.spark

import jp.co.yahoo.yosegi.spread.expression.AndExpressionNode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

case class YosegiScan(
                     sparkSession: SparkSession,
                     hadoopConf: Configuration,
                     fileIndex: PartitioningAwareFileIndex,
                     dataSchema: StructType,
                     readDataSchema: StructType,
                     readPartitionSchema: StructType,
                     options: CaseInsensitiveStringMap,
                     pushedFilters: Array[Filter],
                     partitionFilters: Seq[Expression] = Seq.empty,
                     dataFilters: Seq[Expression] = Seq.empty) extends FileScan {
  override def isSplitable(path: Path): Boolean = {
    true
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    YosegiPartitionReaderFactory(
      sparkSession.sessionState.conf,
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf)),
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      pushedFilters
    )
  }

  override def equals(obj: Any): Boolean = {
    super.equals(obj)
  }

  override def hashCode(): Int = super.hashCode()
}
