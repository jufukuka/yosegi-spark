package jp.co.yahoo.yosegi.spark.reader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.IRawConverter;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import jp.co.yahoo.yosegi.spark.utils.PartitionColumnUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SparkColumnarBatchConverter implements IRawConverter<ColumnarBatch> {
  private final StructType schema;
  private final StructType partitionSchema;
  private final InternalRow partitionValue;
  private final WritableColumnVector[] childColumns;
  private final Map<String, Integer> keyIndexMap;

  public SparkColumnarBatchConverter(
      final StructType schema,
      final StructType partitionSchema,
      final InternalRow partitionValue,
      final Map<String, Integer> keyIndexMap,
      final WritableColumnVector[] childColumns) {
    this.schema = schema;
    this.partitionSchema = partitionSchema;
    this.partitionValue = partitionValue;
    this.keyIndexMap = keyIndexMap;
    this.childColumns = childColumns;
  }

  @Override
  public ColumnarBatch convert(final List<ColumnBinary> raw, final int loadSize) throws IOException {
    // NOTE: initialize
    for (int i = 0; i < childColumns.length; i++) {
      childColumns[i].reset();
      childColumns[i].reserve(loadSize);
    }
    final ColumnarBatch result = new ColumnarBatch(childColumns);
    // NOTE: childColumns
    final boolean[] isSet = new boolean[childColumns.length];
    for (int i = 0; i < raw.size(); i++) {
      final ColumnBinary columnBinary = raw.get(i);
      if (!keyIndexMap.containsKey(columnBinary.columnName)) {
        continue;
      }
      final int index = keyIndexMap.get(columnBinary.columnName);
      isSet[index] = true;
      SparkLoaderFactoryUtil.createLoaderFactory(childColumns[index]).create(columnBinary, loadSize);
    }
    // NOTE: null columns
    for (int i = 0; i < childColumns.length; i++) {
      if (!isSet[i]) {
        childColumns[i].putNulls(0, loadSize);
      }
    }
    // NOTE: partitionColumns
    final WritableColumnVector[] partColumns =
        PartitionColumnUtil.createPartitionColumns(partitionSchema, partitionValue, loadSize);
    for (int i = schema.length(), n = 0; i < childColumns.length; i++, n++) {
      childColumns[i] = partColumns[n];
    }
    result.setNumRows(loadSize);
    return result;
  }
}
