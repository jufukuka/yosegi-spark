package jp.co.yahoo.yosegi.spark.inmemory.factory.map;

import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public abstract class AbstractSparkMapChildLoaderFactory implements ILoaderFactory<WritableColumnVector> {
  protected final WritableColumnVector vector;
  protected final int totalChildCount;
  protected final int currentChildCount;

  public AbstractSparkMapChildLoaderFactory(WritableColumnVector vector, int totalChildCount, int currentChildCount) {
    this.vector = vector;
    this.totalChildCount = totalChildCount;
    this.currentChildCount = currentChildCount;
  }
}
