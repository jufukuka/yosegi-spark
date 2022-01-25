package jp.co.yahoo.yosegi.spark.reader;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.reader.WrapReader;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class SparkColumnarBatchReader implements IColumnarBatchReader {

  private final WrapReader<ColumnarBatch> reader;
  private final YosegiReader yosegiReader;
  private final SparkColumnarBatchConverter converter;
  private final StructType schema;
  private final StructType partitionSchema;
  private final InternalRow partitionValue;
  private final IExpressionNode node;
  private final WritableColumnVector[] childColumns;
  private final Map<String, Integer> keyIndexMap = new HashMap<String, Integer>();

  public SparkColumnarBatchReader(
      final StructType partitionSchema,
      final InternalRow partitionValue,
      final StructType schema,
      final InputStream in,
      final long fileLength,
      final long start,
      final long length,
      final Configuration config,
      final IExpressionNode node)
      throws IOException {
    this.schema = schema;
    this.partitionSchema = partitionSchema;
    this.partitionValue = partitionValue;
    this.node = node;
    StructField[] fields = schema.fields();
    childColumns = new OnHeapColumnVector[schema.length() + partitionSchema.length()];
    for (int i = 0; i < fields.length; i++) {
      keyIndexMap.put(fields[i].name(), i);
      childColumns[i] = new OnHeapColumnVector(0, fields[i].dataType());
    }
    // NOTE: create reader
    yosegiReader = new YosegiReader();
    yosegiReader.setNewStream(in, fileLength, config, start, length);
    yosegiReader.setBlockSkipIndex(node);
    converter =
        new SparkColumnarBatchConverter(
            schema, partitionSchema, partitionValue, keyIndexMap, childColumns);
    reader = new WrapReader<>(yosegiReader, converter);
  }

  @Override
  public void setLineFilterNode(IExpressionNode node) {

  }

  @Override
  public boolean hasNext() throws IOException {
    return reader.hasNext();
  }

  @Override
  public ColumnarBatch next() throws IOException {
    if (!hasNext()) {
      ColumnarBatch result = new ColumnarBatch(childColumns);
      result.setNumRows(0);
      return result;
    }
    return reader.next();
  }

  @Override
  public void close() throws Exception {
    reader.close();
    for (int i = 0; i < childColumns.length; i++) {
      childColumns[i].close();
    }
  }
}
