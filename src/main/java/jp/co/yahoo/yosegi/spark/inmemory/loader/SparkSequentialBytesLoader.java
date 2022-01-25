package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkSequentialBytesLoader extends AbstractSparkSequentialLoader {

  public SparkSequentialBytesLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  public SparkSequentialBytesLoader(WritableColumnVector vector, int loadSize, int totalChildCount, int currentChildCount) {
    super(vector, loadSize, totalChildCount, currentChildCount);
  }

  @Override
  public void setBoolean(int index, boolean value) throws IOException {
    setString(index, new BooleanObj(value).getString());
  }

  @Override
  public void setByte(int index, byte value) throws IOException {
    setString(index, new ByteObj(value).getString());
  }

  @Override
  public void setShort(int index, short value) throws IOException {
    setString(index, new ShortObj(value).getString());
  }

  @Override
  public void setInteger(int index, int value) throws IOException {
    setString(index, new IntegerObj(value).getString());
  }

  @Override
  public void setLong(int index, long value) throws IOException {
    setString(index, new LongObj(value).getString());
  }

  @Override
  public void setFloat(int index, float value) throws IOException {
    setString(index, new FloatObj(value).getString());
  }

  @Override
  public void setDouble(int index, double value) throws IOException {
    setString(index, new DoubleObj(value).getString());
  }

  @Override
  public void setBytes(int index, byte[] value, int start, int length) throws IOException {
    vector.putByteArray(getIndex(index), value, start, length);
  }

  @Override
  public void setString(int index, String value) throws IOException {
    setBytes(index, new StringObj(value).getBytes());
  }
}
