package jp.co.yahoo.yosegi.spark.inmemory.factory.map;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.map.SparkMapChildNullLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.map.SparkMapChildSequentialLongLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkMapChildLongLoaderFactory extends AbstractSparkMapChildLoaderFactory {
  public SparkMapChildLongLoaderFactory(
      WritableColumnVector vector, int totalChildCount, int currentChildCount) {
    super(vector, totalChildCount, currentChildCount);
  }

  @Override
  public ILoader createLoader(ColumnBinary columnBinary, int loadSize) throws IOException {
    if (columnBinary == null) {
      return new SparkMapChildNullLoader(vector, loadSize, totalChildCount, currentChildCount);
    }
    switch (getLoadType(columnBinary, loadSize)) {
      case SEQUENTIAL:
        return new SparkMapChildSequentialLongLoader(
            vector, loadSize, totalChildCount, currentChildCount);
      case DICTIONARY:
      case CONST:
      case UNION:
      default:
        return new SparkMapChildNullLoader(vector, loadSize, totalChildCount, currentChildCount);
    }
  }
}