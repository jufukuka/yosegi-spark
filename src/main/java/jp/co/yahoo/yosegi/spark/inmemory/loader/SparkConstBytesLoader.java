package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IConstLoader;
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

public class SparkConstBytesLoader implements IConstLoader<WritableColumnVector> {

  private class SparkBytesConstDictionary implements ISparkDictionary {

    private byte[] binary;
    private int start;
    private int length;

    public SparkBytesConstDictionary() {
    }

    @Override
    public byte[] decodeToBinary(final int id) {
      byte[] result = new byte[length];
      System.arraycopy(binary, start, result, 0, result.length);
      return result;
    }

    @Override
    public void setBytes(final int id, final byte[] value, final int start, final int length)
        throws IOException {
      binary = value;
      this.start = start;
      this.length = length;
    }
  }

  private final WritableColumnVector vector;
  private final int loadSize;

  private WritableColumnVector idxVector;
  private ISparkDictionary dic;

  public SparkConstBytesLoader(WritableColumnVector vector, int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
    idxVector = vector.reserveDictionaryIds(loadSize);
    dic = new SparkBytesConstDictionary();
    this.vector.setDictionary(dic);
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

  @Override
  public void setConstFromBoolean(boolean value) throws IOException {
    setConstFromString(new BooleanObj(value).getString());
  }

  @Override
  public void setConstFromByte(byte value) throws IOException {
    setConstFromString(new ByteObj(value).getString());
  }

  @Override
  public void setConstFromShort(short value) throws IOException {
    setConstFromString(new ShortObj(value).getString());
  }

  @Override
  public void setConstFromInteger(int value) throws IOException {
    setConstFromString(new IntegerObj(value).getString());
  }

  @Override
  public void setConstFromLong(long value) throws IOException {
    setConstFromString(new LongObj(value).getString());
  }

  @Override
  public void setConstFromFloat(float value) throws IOException {
    setConstFromString(new FloatObj(value).getString());
  }

  @Override
  public void setConstFromDouble(double value) throws IOException {
    setConstFromString(new DoubleObj(value).getString());
  }

  //@Override
  //public void setConstFromBytes(byte[] value) throws IOException {}

  @Override
  public void setConstFromBytes(byte[] value, int start, int length) throws IOException {
    // FIXME: how to use setIsConstant
    dic.setBytes(0, value, start, length);
    idxVector.putInts(0, loadSize, 0);
  }

  @Override
  public void setConstFromString(String value) throws IOException {
    setConstFromBytes(new StringObj(value).getBytes());
  }

  //@Override
  //public void setConstFromString(char[] value) throws IOException {}

  //@Override
  //public void setConstFromString(char[] value, int start, int length) throws IOException {}

  @Override
  public void setConstFromNull() throws IOException {
    // FIXME:
    vector.putNulls(0, loadSize);
  }
}
