package jp.co.yahoo.yosegi.spark

import jp.co.yahoo.yosegi.spark.reader.IColumnarBatchReader

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch

class YosegiPartitionReader(reader: IColumnarBatchReader) extends PartitionReader[ColumnarBatch] {
  override def next(): Boolean = reader.hasNext()

  override def get(): ColumnarBatch = reader.next()

  override def close(): Unit = reader.close()
}
