package jp.co.yahoo.yosegi.spark.inmemory.factory;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkConstIntegerLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkDictionaryIntegerLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkNullLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkSequentialIntegerLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkIntegerLoaderFactory implements ILoaderFactory<WritableColumnVector> {
  private final WritableColumnVector vector;

  public SparkIntegerLoaderFactory(final WritableColumnVector vector) {
    this.vector = vector;
  }

  @Override
  public ILoader createLoader(final ColumnBinary columnBinary, final int loadSize) throws IOException {
    if (columnBinary == null) {
      return new SparkNullLoader(vector, loadSize);
    }
    switch (getLoadType(columnBinary, loadSize)) {
      case SEQUENTIAL:
        return new SparkSequentialIntegerLoader(vector, loadSize);
      case DICTIONARY:
        return new SparkDictionaryIntegerLoader(vector, loadSize);
      case CONST:
        return new SparkConstIntegerLoader(vector, loadSize);
      case UNION:
        // TODO:
      default:
        return new SparkNullLoader(vector, loadSize);
    }
  }
}
