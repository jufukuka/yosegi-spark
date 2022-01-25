package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IConstLoader;
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

public abstract class AbstractSparkConstNumberLoader implements IConstLoader<WritableColumnVector> {
  protected final WritableColumnVector vector;
  protected final int loadSize;

  protected WritableColumnVector idxVector;
  protected ISparkDictionary dic;

  public AbstractSparkConstNumberLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
  }

  public abstract void setConstFromPrimitiveObject(PrimitiveObject value) throws IOException;

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
  public void finish() throws IOException {}

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }

  // @Override
  // public void setConstFromBoolean(boolean value) throws IOException {}

  @Override
  public void setConstFromByte(byte value) throws IOException {
    setConstFromPrimitiveObject(new ByteObj(value));
  }

  @Override
  public void setConstFromShort(short value) throws IOException {
    setConstFromPrimitiveObject(new ShortObj(value));
  }

  @Override
  public void setConstFromInteger(int value) throws IOException {
    setConstFromPrimitiveObject(new IntegerObj(value));
  }

  @Override
  public void setConstFromLong(long value) throws IOException {
    setConstFromPrimitiveObject(new LongObj(value));
  }

  @Override
  public void setConstFromFloat(float value) throws IOException {
    setConstFromPrimitiveObject(new FloatObj(value));
  }

  @Override
  public void setConstFromDouble(double value) throws IOException {
    setConstFromPrimitiveObject(new DoubleObj(value));
  }

  // @Override
  // public void setConstFromBytes(byte[] value) throws IOException {}

  @Override
  public void setConstFromBytes(byte[] value, int start, int length) throws IOException {
    setConstFromPrimitiveObject(new BytesObj(value, start, length));
  }

  @Override
  public void setConstFromString(String value) throws IOException {
    setConstFromPrimitiveObject(new StringObj(value));
  }

  // @Override
  // public void setConstFromString(char[] value) throws IOException {}

  // @Override
  // public void setConstFromString(char[] value, int start, int length) throws IOException {}

  @Override
  public void setConstFromNull() throws IOException {
    // FIXME:
    vector.putNulls(0, loadSize);
  }
}
