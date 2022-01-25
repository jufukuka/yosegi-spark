package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IConstLoader;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.Decimal;

import java.io.IOException;

public class SparkConstDecimalLoader implements IConstLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;

  public SparkConstDecimalLoader(WritableColumnVector vector, int loadSize) {
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
  //  public void setConstFromBoolean(boolean value) throws IOException {
  //    IConstLoader.super.setConstFromBoolean(value);
  //  }

  @Override
  public void setConstFromByte(byte value) throws IOException {
    setConstFromInteger(value);
  }

  @Override
  public void setConstFromShort(short value) throws IOException {
    setConstFromInteger(value);
  }

  @Override
  public void setConstFromInteger(int value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    for (int i = 0; i < loadSize; i++) {
      vector.putDecimal(i, decimal, decimal.precision());
    }
  }

  @Override
  public void setConstFromLong(long value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    for (int i = 0; i < loadSize; i++) {
      vector.putDecimal(i, decimal, decimal.precision());
    }
  }

  @Override
  public void setConstFromFloat(float value) throws IOException {
    try {
      setConstFromDouble(new FloatObj(value).getDouble());
    } catch (Exception e) {
      setConstFromNull();
    }
  }

  @Override
  public void setConstFromDouble(double value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    for (int i = 0; i < loadSize; i++) {
      vector.putDecimal(i, decimal, decimal.precision());
    }
  }

  //  @Override
  //  public void setConstFromBytes(byte[] value) throws IOException {
  //    IConstLoader.super.setConstFromBytes(value);
  //  }

  @Override
  public void setConstFromBytes(byte[] value, int start, int length) throws IOException {
    try {
      setConstFromString(new BytesObj(value, start, length).getString());
    } catch (Exception e) {
      setConstFromNull();
    }
  }

  @Override
  public void setConstFromString(String value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    for (int i = 0; i < 0; i++) {
      vector.putDecimal(i, decimal, decimal.precision());
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
