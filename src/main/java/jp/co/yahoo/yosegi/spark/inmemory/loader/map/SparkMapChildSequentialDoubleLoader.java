package jp.co.yahoo.yosegi.spark.inmemory.loader.map;

import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkSequentialDoubleLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public class SparkMapChildSequentialDoubleLoader extends SparkSequentialDoubleLoader {
  public SparkMapChildSequentialDoubleLoader(WritableColumnVector vector, int loadSize, int totalChildCount, int currentChildCount) {
    super(vector, loadSize, totalChildCount, currentChildCount);
  }

  @Override
  protected int getIndex(int index) {
    return index * totalChildCount + currentChildCount;
  }
}
