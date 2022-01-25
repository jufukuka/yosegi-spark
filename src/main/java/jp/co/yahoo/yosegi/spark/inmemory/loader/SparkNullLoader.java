package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkNullLoader implements ISequentialLoader<WritableColumnVector> {

  private final WritableColumnVector vector;
  private final int loadSize;

  public SparkNullLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
  }

  @Override
  public LoadType getLoaderType() {
    return LoadType.NULL;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(int index) throws IOException {
    // TODO:
  }

  @Override
  public void finish() throws IOException {
    // FIXME:
    vector.putNulls(0, loadSize);
  }

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }
}
