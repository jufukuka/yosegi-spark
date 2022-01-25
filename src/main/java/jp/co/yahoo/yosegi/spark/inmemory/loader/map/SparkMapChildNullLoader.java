package jp.co.yahoo.yosegi.spark.inmemory.loader.map;

import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.spark.inmemory.loader.AbstractSparkSequentialLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkMapChildNullLoader extends AbstractSparkSequentialLoader {
  public SparkMapChildNullLoader(WritableColumnVector vector, int loadSize, int totalChildCount, int currentChildCount) {
    super(vector, loadSize, totalChildCount, currentChildCount);
  }

  @Override
  protected int getIndex(int index) {
    return index * totalChildCount + currentChildCount;
  }

  @Override
  public void finish() throws IOException {
    for (int i = 0; i < loadSize; i++) {
      setNull(i);
    }
  }

  @Override
  public LoadType getLoaderType() {
    return LoadType.NULL;
  }
}
