package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import jp.co.yahoo.yosegi.spark.inmemory.SparkMapChildLoaderFactoryUtil;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.MapType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SparkMapLoader implements ISpreadLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;

  private int totalChildCount;
  private int currentChildCount;

  public SparkMapLoader(WritableColumnVector vector, int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
    //MapType mt = (MapType) vector.dataType();
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(int index) throws IOException {
    vector.putNull(index);
  }

  @Override
  public void finish() throws IOException {
    // FIXME:
  }

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }

  @Override
  public void setChildCount(int childCount) throws IOException {
    final int capacity = loadSize * childCount;
    vector.getChild(0).reserve(capacity);
    vector.getChild(1).reserve(capacity);
    totalChildCount = childCount;
    currentChildCount = 0;
    for (int i = 0; i < loadSize; i++) {
      vector.putArray(i, i * totalChildCount, totalChildCount);
    }
  }

  @Override
  public void loadChild(ColumnBinary columnBinary, int loadSize) throws IOException {
    for (int i = 0; i < loadSize; i++) {
      vector.getChild(0).putByteArray(i * totalChildCount + currentChildCount, columnBinary.columnName.getBytes(StandardCharsets.UTF_8));
    }
    SparkMapChildLoaderFactoryUtil.createLoaderFactory(
            vector.getChild(1), totalChildCount, currentChildCount)
        .create(columnBinary, loadSize);
    currentChildCount++;
  }
}
