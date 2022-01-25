package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkConstShortLoader extends AbstractSparkConstNumberLoader {
  public SparkConstShortLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setConstFromPrimitiveObject(PrimitiveObject value) throws IOException {
    if (value == null) {
      setConstFromNull();
    } else {
      try {
        setConstFromShort(value.getShort());
      } catch (Exception e) {
        setConstFromNull();
      }
    }
  }

  @Override
  public void setConstFromShort(short value) throws IOException {
    vector.putShorts(0, loadSize, value);
  }
}
