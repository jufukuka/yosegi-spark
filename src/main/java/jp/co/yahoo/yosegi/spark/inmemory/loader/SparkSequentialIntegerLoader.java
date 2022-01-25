package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkSequentialIntegerLoader extends AbstractSparkSequentialNumberLoader {
  public SparkSequentialIntegerLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  public SparkSequentialIntegerLoader(WritableColumnVector vector, int loadSize, int totalChildCount, int currentChildCount) {
    super(vector, loadSize, totalChildCount, currentChildCount);
  }

  @Override
  public void setPrimitiveObject(int index, PrimitiveObject value) throws IOException {
    if (value == null) {
      setNull(index);
    } else {
      try {
        setInteger(index, value.getInt());
      } catch (Exception e) {
        setNull(index);
      }
    }
  }

  @Override
  public void setInteger(int index, int value) throws IOException {
    vector.putInt(getIndex(index), value);
  }
}
