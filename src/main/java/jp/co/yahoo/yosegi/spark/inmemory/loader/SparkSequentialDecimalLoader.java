package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.Decimal;

import java.io.IOException;

public class SparkSequentialDecimalLoader extends AbstractSparkSequentialLoader {
  public SparkSequentialDecimalLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  public SparkSequentialDecimalLoader(WritableColumnVector vector, int loadSize, int totalChildCount, int currentChildCount) {
    super(vector, loadSize, totalChildCount, currentChildCount);
  }

//  @Override
//  public void setBoolean(int index, boolean value) throws IOException {
//    ISequentialLoader.super.setBoolean(index, value);
//  }

  @Override
  public void setByte(int index, byte value) throws IOException {
    setInteger(index, value);
  }

  @Override
  public void setShort(int index, short value) throws IOException {
    setInteger(index, value);
  }

  @Override
  public void setInteger(int index, int value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    vector.putDecimal(getIndex(index), decimal, decimal.precision());
  }

  @Override
  public void setLong(int index, long value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    vector.putDecimal(getIndex(index), decimal, decimal.precision());
  }

  @Override
  public void setFloat(int index, float value) throws IOException {
    try {
      setDouble(index, new FloatObj(value).getDouble());
    } catch (Exception e) {
      setNull(index);
    }
  }

  @Override
  public void setDouble(int index, double value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    vector.putDecimal(getIndex(index), decimal, decimal.precision());
  }

  //  @Override
  //  public void setBytes(int index, byte[] value) throws IOException {
  //    ISequentialLoader.super.setBytes(index, value);
  //  }

  @Override
  public void setBytes(int index, byte[] value, int start, int length) throws IOException {
    try {
      setString(index, new BytesObj(value, start, length).getString());
    } catch (Exception e) {
      setNull(index);
    }
  }

  @Override
  public void setString(int index, String value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    vector.putDecimal(getIndex(index), decimal, decimal.precision());
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
