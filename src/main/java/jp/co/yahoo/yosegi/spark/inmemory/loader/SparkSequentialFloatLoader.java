package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkSequentialFloatLoader extends AbstractSparkSequentialNumberLoader {
  public SparkSequentialFloatLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  public SparkSequentialFloatLoader(WritableColumnVector vector, int loadSize, int totalChildCount, int currentChildCount) {
    super(vector, loadSize, totalChildCount, currentChildCount);
  }

  @Override
  public void setPrimitiveObject(int index, PrimitiveObject value) throws IOException {
    if (value == null) {
      setNull(index);
    } else {
      try {
        setFloat(index, value.getFloat());
      } catch (Exception e) {
        setNull(index);
      }
    }
  }

  @Override
  public void setFloat(int index, float value) throws IOException {
    vector.putFloat(getIndex(index), value);
  }
}
