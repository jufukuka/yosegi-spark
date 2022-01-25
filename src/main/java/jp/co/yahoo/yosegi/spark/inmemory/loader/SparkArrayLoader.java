package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.IArrayLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkLoaderFactoryUtil;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkArrayLoader implements IArrayLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;

  public SparkArrayLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(final int index) throws IOException {
    vector.putNull(index);
  }

  @Override
  public void finish() throws IOException {
    // FIXME:
  }

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }

  @Override
  public void setArrayIndex(final int index, final int start, final int length) throws IOException {
    vector.putArray(index, start, length);
  }

  @Override
  public void loadChild(final ColumnBinary columnBinary, final int childLength) throws IOException {
    vector.getChild(0).reserve(childLength);
    SparkLoaderFactoryUtil.createLoaderFactory(vector.getChild(0))
        .create(columnBinary, childLength);
  }
}
