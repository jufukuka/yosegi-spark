package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IConstLoader;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkConstBooleanLoader implements IConstLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;

  public SparkConstBooleanLoader(WritableColumnVector vector, int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(int index) throws IOException {
    // FIXME:
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

  //  @Override
  //  public LoadType getLoaderType() {
  //    return IConstLoader.super.getLoaderType();
  //  }

  @Override
  public void setConstFromBoolean(boolean value) throws IOException {
    vector.putBooleans(0, loadSize, value);
  }

  //  @Override
  //  public void setConstFromByte(byte value) throws IOException {
  //    IConstLoader.super.setConstFromByte(value);
  //  }
  //
  //  @Override
  //  public void setConstFromShort(short value) throws IOException {
  //    IConstLoader.super.setConstFromShort(value);
  //  }
  //
  //  @Override
  //  public void setConstFromInteger(int value) throws IOException {
  //    IConstLoader.super.setConstFromInteger(value);
  //  }
  //
  //  @Override
  //  public void setConstFromLong(long value) throws IOException {
  //    IConstLoader.super.setConstFromLong(value);
  //  }
  //
  //  @Override
  //  public void setConstFromFloat(float value) throws IOException {
  //    IConstLoader.super.setConstFromFloat(value);
  //  }
  //
  //  @Override
  //  public void setConstFromDouble(double value) throws IOException {
  //    IConstLoader.super.setConstFromDouble(value);
  //  }
  //
  //  @Override
  //  public void setConstFromBytes(byte[] value) throws IOException {
  //    IConstLoader.super.setConstFromBytes(value);
  //  }

  @Override
  public void setConstFromBytes(byte[] value, int start, int length) throws IOException {
    try {
      setConstFromBoolean(new BytesObj(value, start, length).getBoolean());
    } catch (Exception e) {
      setConstFromNull();
    }
  }

  @Override
  public void setConstFromString(String value) throws IOException {
    try {
      setConstFromBoolean(new StringObj(value).getBoolean());
    } catch (Exception e) {
      setConstFromNull();
    }
  }

  //  @Override
  //  public void setConstFromString(char[] value) throws IOException {
  //    IConstLoader.super.setConstFromString(value);
  //  }
  //
  //  @Override
  //  public void setConstFromString(char[] value, int start, int length) throws IOException {
  //    IConstLoader.super.setConstFromString(value, start, length);
  //  }

  @Override
  public void setConstFromNull() throws IOException {
    vector.putNulls(0, loadSize);
  }
}
