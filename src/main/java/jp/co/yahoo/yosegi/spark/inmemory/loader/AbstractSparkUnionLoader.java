package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.IUnionLoader;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class AbstractSparkUnionLoader implements IUnionLoader<WritableColumnVector> {
  protected final WritableColumnVector vector;
  protected final int loadSize;

  public AbstractSparkUnionLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(int index) throws IOException {
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
  public void setIndexAndColumnType(int index, ColumnType columnType) throws IOException {
    // FIXME:
  }

  @Override
  public void loadChild(ColumnBinary columnBinary, int childLoadSize) throws IOException {
    // FIXME:
  }
}
