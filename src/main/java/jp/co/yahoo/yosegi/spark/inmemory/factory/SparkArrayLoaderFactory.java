package jp.co.yahoo.yosegi.spark.inmemory.factory;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkArrayLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkNullLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkArrayLoaderFactory implements ILoaderFactory<WritableColumnVector> {
  private final WritableColumnVector vector;

  public SparkArrayLoaderFactory(final WritableColumnVector vector) {
    this.vector = vector;
  }

  @Override
  public ILoader createLoader(ColumnBinary columnBinary, int loadSize) throws IOException {
    if (columnBinary == null) {
      // FIXME:
      return new SparkNullLoader(vector, loadSize);
    }
    switch (getLoadType(columnBinary, loadSize)) {
      case ARRAY:
        return new SparkArrayLoader(vector, loadSize);
      default:
        // FIXME:
        return new SparkNullLoader(vector, loadSize);
    }
  }
}
