package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
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

public class SparkDictionaryBytesLoader implements IDictionaryLoader<WritableColumnVector> {

  private class SparkBytesDictionary implements ISparkDictionary {

    private final byte[][] binaryLinkArray;
    private final int[] startArray;
    private final int[] lengthArray;

    public SparkBytesDictionary(final int dicSize) {
      binaryLinkArray = new byte[dicSize][];
      startArray = new int[dicSize];
      lengthArray = new int[dicSize];
    }

    @Override
    public byte[] decodeToBinary(final int id) {
      byte[] result = new byte[lengthArray[id]];
      System.arraycopy(binaryLinkArray[id], startArray[id], result, 0, result.length);
      return result;
    }

    @Override
    public void setBytes(final int id, final byte[] value, final int start, final int length)
        throws IOException {
      binaryLinkArray[id] = value;
      startArray[id] = start;
      lengthArray[id] = length;
    }
  }

  private final WritableColumnVector vector;
  private final int loadSize;

  private WritableColumnVector idxVector;
  private ISparkDictionary dic;

  public SparkDictionaryBytesLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(int index) throws IOException {
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
  public void createDictionary(int dictionarySize) throws IOException {
    idxVector = vector.reserveDictionaryIds(loadSize);
    dic = new SparkBytesDictionary(dictionarySize);
    vector.setDictionary(dic);
  }

  @Override
  public void setDictionaryIndex(int index, int dicIndex) throws IOException {
    idxVector.putInt(index, dicIndex);
  }

  @Override
  public void setBooleanToDic(int index, boolean value) throws IOException {
    setStringToDic(index, new BooleanObj(value).getString());
  }

  @Override
  public void setByteToDic(int index, byte value) throws IOException {
    setStringToDic(index, new ByteObj(value).getString());
  }

  @Override
  public void setShortToDic(int index, short value) throws IOException {
    setStringToDic(index, new ShortObj(value).getString());
  }

  @Override
  public void setIntegerToDic(int index, int value) throws IOException {
    setStringToDic(index, new IntegerObj(value).getString());
  }

  @Override
  public void setLongToDic(int index, long value) throws IOException {
    setStringToDic(index, new LongObj(value).getString());
  }

  @Override
  public void setFloatToDic(int index, float value) throws IOException {
    setStringToDic(index, new FloatObj(value).getString());
  }

  @Override
  public void setDoubleToDic(int index, double value) throws IOException {
    setStringToDic(index, new DoubleObj(value).getString());
  }

  // @Override
  // public void setBytesToDic(int index, byte[] value) throws IOException {}

  @Override
  public void setBytesToDic(int index, byte[] value, int start, int length) throws IOException {
    dic.setBytes(index, value, start, length);
  }

  @Override
  public void setStringToDic(int index, String value) throws IOException {
    setBytesToDic(index, new StringObj(value).getBytes());
  }

  // @Override
  // public void setStringToDic(int index, char[] value) throws IOException {}

  // @Override
  // public void setStringToDic(int index, char[] value, int start, int length) throws IOException
  // {}

  @Override
  public void setNullToDic(int index) throws IOException {
    // FIXME: this method is not used in yosegi
  }
}
