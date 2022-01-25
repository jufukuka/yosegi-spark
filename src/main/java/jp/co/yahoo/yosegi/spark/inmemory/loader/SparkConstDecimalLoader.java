package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

import java.io.IOException;

public class SparkConstDecimalLoader extends AbstractSparkConstLoader {
  private final int precision;
  private final int scale;

  public SparkConstDecimalLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
    precision = ((DecimalType) vector.dataType()).precision();
    scale = ((DecimalType) vector.dataType()).scale();
  }

  //  @Override
  //  public void setConstFromBoolean(boolean value) throws IOException {
  //    IConstLoader.super.setConstFromBoolean(value);
  //  }

  @Override
  public void setConstFromByte(final byte value) throws IOException {
    setConstFromInteger(value);
  }

  @Override
  public void setConstFromShort(final short value) throws IOException {
    setConstFromInteger(value);
  }

  @Override
  public void setConstFromInteger(final int value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    for (int i = 0; i < loadSize; i++) {
      vector.putDecimal(i, decimal, precision);
    }
  }

  @Override
  public void setConstFromLong(final long value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    for (int i = 0; i < loadSize; i++) {
      vector.putDecimal(i, decimal, precision);
    }
  }

  @Override
  public void setConstFromFloat(final float value) throws IOException {
    try {
      setConstFromDouble(new FloatObj(value).getDouble());
    } catch (final Exception e) {
      setConstFromNull();
    }
  }

  @Override
  public void setConstFromDouble(final double value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    for (int i = 0; i < loadSize; i++) {
      vector.putDecimal(i, decimal, precision);
    }
  }

  //  @Override
  //  public void setConstFromBytes(byte[] value) throws IOException {
  //    IConstLoader.super.setConstFromBytes(value);
  //  }

  @Override
  public void setConstFromBytes(final byte[] value, final int start, final int length)
      throws IOException {
    try {
      setConstFromString(new BytesObj(value, start, length).getString());
    } catch (final Exception e) {
      setConstFromNull();
    }
  }

  @Override
  public void setConstFromString(final String value) throws IOException {
    final Decimal decimal = Decimal.apply(value);
    decimal.changePrecision(precision, scale);
    for (int i = 0; i < 0; i++) {
      vector.putDecimal(i, decimal, precision);
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
