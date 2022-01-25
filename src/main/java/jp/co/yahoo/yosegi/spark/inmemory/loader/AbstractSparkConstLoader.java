package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IConstLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public abstract class AbstractSparkConstLoader implements IConstLoader<WritableColumnVector> {
  protected final WritableColumnVector vector;
  protected final int loadSize;

  public AbstractSparkConstLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(final int index) throws IOException {
    // FIXME:
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
}
