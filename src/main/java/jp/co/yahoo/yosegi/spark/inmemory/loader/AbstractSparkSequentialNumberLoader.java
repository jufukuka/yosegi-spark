package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public abstract class AbstractSparkSequentialNumberLoader extends AbstractSparkSequentialLoader {

  public AbstractSparkSequentialNumberLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  public AbstractSparkSequentialNumberLoader(WritableColumnVector vector, int loadSize, int totalChildCount, int currentChildCount) {
    super(vector, loadSize, totalChildCount, currentChildCount);
  }

  public abstract void setPrimitiveObject(int index, PrimitiveObject value) throws IOException;

  // @Override
  // public void setBoolean(int index, boolean value) throws IOException {}

  @Override
  public void setByte(int index, byte value) throws IOException {
    setPrimitiveObject(index, new ByteObj(value));
  }

  @Override
  public void setShort(int index, short value) throws IOException {
    setPrimitiveObject(index, new ShortObj(value));
  }

  @Override
  public void setInteger(int index, int value) throws IOException {
    setPrimitiveObject(index, new IntegerObj(value));
  }

  @Override
  public void setLong(int index, long value) throws IOException {
    setPrimitiveObject(index, new LongObj(value));
  }

  @Override
  public void setFloat(int index, float value) throws IOException {
    setPrimitiveObject(index, new FloatObj(value));
  }

  @Override
  public void setDouble(int index, double value) throws IOException {
    setPrimitiveObject(index, new DoubleObj(value));
  }

  // @Override
  // public void setBytes(int index, byte[] value) throws IOException {}

  @Override
  public void setBytes(int index, byte[] value, int start, int length) throws IOException {
    setPrimitiveObject(index, new BytesObj(value, start, length));
  }

  @Override
  public void setString(int index, String value) throws IOException {
    setPrimitiveObject(index, new StringObj(value));
  }

  // @Override
  // public void setString(int index, char[] value) throws IOException {}

  // @Override
  // public void setString(int index, char[] value, int start, int length) throws IOException {}
}
