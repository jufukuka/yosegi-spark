package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
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

public abstract class AbstractSparkDictionaryNumberLoader
    implements IDictionaryLoader<WritableColumnVector> {
  protected final WritableColumnVector vector;
  protected final int loadSize;

  protected WritableColumnVector idxVector;
  protected ISparkDictionary dic;
  protected boolean[] isNull;

  public AbstractSparkDictionaryNumberLoader(
      final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
  }

  public abstract void setPrimitiveObjectToDic(int index, PrimitiveObject value) throws IOException;

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

  // @Override
  // public void createDictionary(int dictionarySize) throws IOException {}

  @Override
  public void setDictionaryIndex(int index, int dicIndex) throws IOException {
    if (isNull[dicIndex]) {
      setNull(index);
    } else {
      idxVector.putInt(index, dicIndex);
    }
  }

  // @Override
  // public void setBooleanToDic(int index, boolean value) throws IOException {}

  @Override
  public void setByteToDic(int index, byte value) throws IOException {
    setPrimitiveObjectToDic(index, new ByteObj(value));
  }

  @Override
  public void setShortToDic(int index, short value) throws IOException {
    setPrimitiveObjectToDic(index, new ShortObj(value));
  }

  @Override
  public void setIntegerToDic(int index, int value) throws IOException {
    setPrimitiveObjectToDic(index, new IntegerObj(value));
  }

  @Override
  public void setLongToDic(int index, long value) throws IOException {
    setPrimitiveObjectToDic(index, new LongObj(value));
  }

  @Override
  public void setFloatToDic(int index, float value) throws IOException {
    setPrimitiveObjectToDic(index, new FloatObj(value));
  }

  @Override
  public void setDoubleToDic(int index, double value) throws IOException {
    setPrimitiveObjectToDic(index, new DoubleObj(value));
  }

  // @Override
  // public void setBytesToDic(int index, byte[] value) throws IOException {}

  @Override
  public void setBytesToDic(int index, byte[] value, int start, int length) throws IOException {
    setPrimitiveObjectToDic(index, new BytesObj(value, start, length));
  }

  @Override
  public void setStringToDic(int index, String value) throws IOException {
    setPrimitiveObjectToDic(index, new StringObj(value));
  }

  // @Override
  // public void setStringToDic(int index, char[] value) throws IOException {}

  // @Override
  // public void setStringToDic(int index, char[] value, int start, int length) throws IOException
  // {}

  @Override
  public void setNullToDic(int index) throws IOException {
    // FIXME: this method is not used in yosegi
    isNull[index] = true;
  }
}
