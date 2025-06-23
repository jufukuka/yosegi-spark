package jp.co.yahoo.yosegi.spark

import jp.co.yahoo.yosegi.config.Configuration
import jp.co.yahoo.yosegi.spark.reader.SparkColumnarBatchReader
import jp.co.yahoo.yosegi.spark.utils.ProjectionPushdownUtil
import jp.co.yahoo.yosegi.spread.expression.AndExpressionNode
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import java.net.URI

case class YosegiPartitionReaderFactory(
                                       sqlConf: SQLConf,
                                       broadcastedConf: Broadcast[SerializableConfiguration],
                                       dataSchema: StructType,
                                       readDataSchema: StructType,
                                       partitionSchema: StructType,
                                       filters: Array[Filter]
                                       ) extends FilePartitionReaderFactory {
  override def supportColumnarReads(partition: InputPartition): Boolean = {
    true
  }

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new RuntimeException("call buildReader")
  }

  override def buildColumnarReader(partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    //val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    //val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    //val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    //val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    // FIXED: requiredSchema
    val projectionPushdownJson = ProjectionPushdownUtil.createProjectionPushdownJson(dataSchema)
    val requiredSchemaJson = dataSchema.json
    val partitionSchemaJson = partitionSchema.json
    //val expandOption: Option[String] = caseSensitiveMap.get("spread.reader.expand.column")
    //val flattenOption: Option[String] = caseSensitiveMap.get("spread.reader.flatten.column")
    //val enableArrowReader: Option[String] = caseSensitiveMap.get("spread.yosegi.enable.arrow.reader")

    val node = new AndExpressionNode()
    //filters.map(FilterConnectorFactory.get(_)).filter(_ != null).foreach(node.addChildNode(_))
    val readSchema: DataType = DataType.fromJson(requiredSchemaJson)
    val partSchema: DataType = DataType.fromJson(partitionSchemaJson)
    val path: Path = new Path(new URI(partitionedFile.filePath))
    val fs: FileSystem = path.getFileSystem(broadcastedConf.value.value)
    val yosegiConfig = new Configuration()
    /*
    if (expandOption.nonEmpty) {
      yosegiConfig.set("spread.reader.expand.column", expandOption.get)
    }
    if (flattenOption.nonEmpty) {
      yosegiConfig.set("spread.reader.flatten.column", flattenOption.get)
    }
     */
    yosegiConfig.set("spread.reader.read.column.names", projectionPushdownJson)

    val reader = new SparkColumnarBatchReader(partSchema.asInstanceOf[StructType], partitionedFile.partitionValues, readSchema.asInstanceOf[StructType], fs.open(path), fs.getFileStatus(path).getLen(), partitionedFile.start, partitionedFile.length, yosegiConfig, node)
    reader.setLineFilterNode(node)
    new YosegiPartitionReader(reader)
  }
}
