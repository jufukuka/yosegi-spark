package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkSequentialBooleanLoader extends AbstractSparkSequentialLoader {
  public SparkSequentialBooleanLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  //  @Override
  //  public LoadType getLoaderType() {
  //    return ISequentialLoader.super.getLoaderType();
  //  }

  @Override
  public void setBoolean(int index, boolean value) throws IOException {
    vector.putBoolean(index, value);
  }

  //  @Override
  //  public void setByte(int index, byte value) throws IOException {
  //    ISequentialLoader.super.setByte(index, value);
  //  }
  //
  //  @Override
  //  public void setShort(int index, short value) throws IOException {
  //    ISequentialLoader.super.setShort(index, value);
  //  }
  //
  //  @Override
  //  public void setInteger(int index, int value) throws IOException {
  //    ISequentialLoader.super.setInteger(index, value);
  //  }
  //
  //  @Override
  //  public void setLong(int index, long value) throws IOException {
  //    ISequentialLoader.super.setLong(index, value);
  //  }
  //
  //  @Override
  //  public void setFloat(int index, float value) throws IOException {
  //    ISequentialLoader.super.setFloat(index, value);
  //  }
  //
  //  @Override
  //  public void setDouble(int index, double value) throws IOException {
  //    ISequentialLoader.super.setDouble(index, value);
  //  }
  //
  //  @Override
  //  public void setBytes(int index, byte[] value) throws IOException {
  //    ISequentialLoader.super.setBytes(index, value);
  //  }

  @Override
  public void setBytes(int index, byte[] value, int start, int length) throws IOException {
    try {
      setBoolean(index, new BytesObj(value, start, length).getBoolean());
    } catch (Exception e) {
      setNull(index);
    }
  }

  @Override
  public void setString(int index, String value) throws IOException {
    try {
      setBoolean(index, new StringObj(value).getBoolean());
    } catch (Exception e) {
      setNull(index);
    }
  }

  //  @Override
  //  public void setString(int index, char[] value) throws IOException {
  //    ISequentialLoader.super.setString(index, value);
  //  }
  //
  //  @Override
  //  public void setString(int index, char[] value, int start, int length) throws IOException {
  //    ISequentialLoader.super.setString(index, value, start, length);
  //  }
}
