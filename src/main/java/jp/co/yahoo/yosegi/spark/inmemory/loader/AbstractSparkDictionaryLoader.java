package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public abstract class AbstractSparkDictionaryLoader
    implements IDictionaryLoader<WritableColumnVector> {
  protected final WritableColumnVector vector;
  protected final int loadSize;

  public AbstractSparkDictionaryLoader(final WritableColumnVector vector, final int loadSize) {
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
}
