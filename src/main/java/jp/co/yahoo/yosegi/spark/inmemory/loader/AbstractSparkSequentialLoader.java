package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public abstract class AbstractSparkSequentialLoader implements ISequentialLoader<WritableColumnVector> {
  protected final WritableColumnVector vector;
  protected final int loadSize;
  protected final int totalChildCount;
  protected final int currentChildCount;

  public AbstractSparkSequentialLoader(WritableColumnVector vector, int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
    this.totalChildCount = 0;
    this.currentChildCount = 0;
  }

  public AbstractSparkSequentialLoader(WritableColumnVector vector, int loadSize, int totalChildCount, int currentChildCount) {
    this.vector = vector;
    this.loadSize = loadSize;
    this.totalChildCount = totalChildCount;
    this.currentChildCount = currentChildCount;
  }

  protected int getIndex(final int index) {
    //return index * totalChildCount + currentChildCount;
    return index;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(int index) throws IOException {
    vector.putNull(getIndex(index));
  }

  @Override
  public void finish() throws IOException {
    //
  }

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }
}
