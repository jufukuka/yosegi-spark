package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkSequentialByteLoader extends AbstractSparkSequentialNumberLoader {
  public SparkSequentialByteLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObject(int index, PrimitiveObject value) throws IOException {
    if (value == null) {
      setNull(index);
    } else {
      try {
        setByte(index, value.getByte());
      } catch (Exception e) {
        setNull(index);
      }
    }
  }

  @Override
  public void setByte(int index, byte value) throws IOException {
    vector.putByte(index, value);
  }
}
