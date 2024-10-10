package jp.co.yahoo.yosegi.spark.inmemory.loader;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryNullArrayLoader extends AbstractSparkDictionaryLoader {

    public SparkDictionaryNullArrayLoader( final WritableColumnVector vector , final int loadSize ) {
        super( vector , loadSize );
    }

    @Override
    public void setNull( final int index ) {
        vector.putArray(index, 0, 0);
    }

    @Override
    public void createDictionary(final int dictionarySize) throws IOException {
        // FIXME
    }

    @Override
    public void setNullToDic(final int index) throws IOException {
        // FIXME
    }

    @Override
    public void setDictionaryIndex( final int index , final int dicIndex ) throws IOException {
        setNull(index);
    }

    @Override
    public void setBooleanToDic( final int index , final boolean value ) throws IOException {
        // FIXME
    }

    @Override
    public void setByteToDic( final int index , final byte value ) throws IOException {
        // FIXME
    }

    @Override
    public void setShortToDic( final int index , final short value ) throws IOException {
        // FIXME
    }

    @Override
    public void setIntegerToDic( final int index , final int value ) throws IOException {
        // FIXME
    }
}
