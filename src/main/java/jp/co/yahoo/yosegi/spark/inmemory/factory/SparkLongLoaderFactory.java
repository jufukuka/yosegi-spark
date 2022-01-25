package jp.co.yahoo.yosegi.spark.inmemory.factory;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkConstLongLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkDictionaryLongLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkNullLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkSequentialLongLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkLongLoaderFactory implements ILoaderFactory<WritableColumnVector> {
  private final WritableColumnVector vector;

  public SparkLongLoaderFactory(final WritableColumnVector vector) {
    this.vector = vector;
  }

  @Override
  public ILoader createLoader(final ColumnBinary columnBinary, final int loadSize) throws IOException {
    if (columnBinary == null) {
      return new SparkNullLoader(vector, loadSize);
    }
    switch (getLoadType(columnBinary, loadSize)) {
      case SEQUENTIAL:
        return new SparkSequentialLongLoader(vector, loadSize);
      case DICTIONARY:
        return new SparkDictionaryLongLoader(vector, loadSize);
      case CONST:
        return new SparkConstLongLoader(vector, loadSize);
      case UNION:
        // TODO:
      default:
        return new SparkNullLoader(vector, loadSize);
    }
  }
}
