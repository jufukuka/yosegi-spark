package jp.co.yahoo.yosegi.spark.inmemory.loader.map;

import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkSequentialDecimalLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public class SparkMapChildSequentialDecimalLoader extends SparkSequentialDecimalLoader {
  public SparkMapChildSequentialDecimalLoader(WritableColumnVector vector, int loadSize, int totalChildCount, int currentChildCount) {
    super(vector, loadSize, totalChildCount, currentChildCount);
  }

  @Override
  protected int getIndex(int index) {
    return index * totalChildCount + currentChildCount;
  }
}