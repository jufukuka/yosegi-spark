package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkSequentialLongLoader extends AbstractSparkSequentialNumberLoader {
  public SparkSequentialLongLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  public SparkSequentialLongLoader(WritableColumnVector vector, int loadSize, int totalChildCount, int currentChildCount) {
    super(vector, loadSize, totalChildCount, currentChildCount);
  }

  @Override
  public void setPrimitiveObject(int index, PrimitiveObject value) throws IOException {
    if (value == null) {
      setNull(index);
    } else {
      try {
        setLong(index, value.getLong());
      } catch (Exception e) {
        setNull(index);
      }
    }
  }

  @Override
  public void setLong(int index, long value) throws IOException {
    vector.putLong(getIndex(index), value);
  }
}
