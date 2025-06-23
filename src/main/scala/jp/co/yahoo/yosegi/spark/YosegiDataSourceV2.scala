package jp.co.yahoo.yosegi.spark

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class YosegiDataSourceV2 extends FileDataSourceV2 {
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[YosegiFileFormat]

  override def shortName(): String = "yosegi"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionWithoutPaths = getOptionsWithoutPaths(options)
    //val hoge: ParquetDataSourceV2
    //val fuga: JsonDataSourceV2
    YosegiTable(tableName, sparkSession, optionWithoutPaths, paths, None, fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionWithoutPaths = getOptionsWithoutPaths(options)
    YosegiTable(tableName, sparkSession, optionWithoutPaths, paths, Some(schema), fallbackFileFormat)
  }
}
