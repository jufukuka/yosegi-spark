package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.IRunLengthEncodingArrayLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkRunLengthEncodingArrayLoader
    implements IRunLengthEncodingArrayLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;

  private int rowId;
  private int offset;

  public SparkRunLengthEncodingArrayLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void finish() throws IOException {}

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }

  @Override
  public void setNull(final int index) throws IOException {
    vector.putNull(index);
  }

  @Override
  public void setRowGroupCount(final int count) throws IOException {}

  @Override
  public void setNullAndRepetitions(
      final int startIndex, final int repetitions, final int rowGroupIndex) throws IOException {
    for (int i = 0; i < repetitions; i++) {
      vector.putNull(rowId);
      rowId++;
    }
  }

  @Override
  public void setRowGourpIndexAndRepetitions(
      final int startIndex,
      final int repetitions,
      final int rowGroupIndex,
      final int rowGroupStart,
      final int rowGourpLength)
      throws IOException {
    for (int i = 0; i < repetitions; i++) {
      vector.putArray(rowId, offset, rowGourpLength);
      rowId++;
    }
    offset += rowGourpLength;
  }

  @Override
  public void loadChild(final ColumnBinary columnBinary, final int childLength) throws IOException {
    vector.getChild(0).reserve(childLength);
    SparkLoaderFactoryUtil.createLoaderFactory(vector.getChild(0))
        .create(columnBinary, childLength);
  }
}
