package jp.co.yahoo.yosegi.spark

import jp.co.yahoo.yosegi.config.Configuration
import jp.co.yahoo.yosegi.spark.schema.SchemaFactory

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class YosegiTable(
                 name: String,
                 sparkSession: SparkSession,
                 options: CaseInsensitiveStringMap,
                 paths: Seq[String],
                 userSpecifiedSchema: Option[StructType],
                 fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new YosegiScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val opts = options.asCaseSensitiveMap.asScala.toMap
    val expandOption: Option[String] = opts.get("spread.reader.expand.column")
    val flattenOption: Option[String] = opts.get("spread.reader.flatten.column")
    val yosegiConfig = new Configuration()
    if (expandOption.nonEmpty) {
      yosegiConfig.set("spread.reader.expand.column", expandOption.get)
    }
    if (flattenOption.nonEmpty) {
      yosegiConfig.set("spread.reader.flatten.column", flattenOption.get)
    }
    Some(SchemaFactory.create(sparkSession, yosegiConfig, files.toArray))
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder {}
  }

  override def supportsDataType(dataType: DataType): Boolean = {
    super.supportsDataType(dataType)
  }

  override def formatName: String = "Yosegi"
}
