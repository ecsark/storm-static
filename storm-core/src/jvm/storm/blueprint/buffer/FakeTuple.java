package storm.blueprint.buffer;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.List;

/**
 * User: ecsark
 * Date: 4/23/14
 * Time: 11:04 AM
 */

/*
    A Tuple implementation written for hierarchical TupleBuffer.
     Field is not supported.
 */
public class FakeTuple implements Tuple, Serializable {

    List<Object> values;

    public FakeTuple(List<Object> values) {
        this.values = values;
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public int fieldIndex(String field) {
        return 0;
    }

    @Override
    public boolean contains(String field) {
        return false;
    }

    public Object getValue(int i) {
        return values.get(i);
    }

    public String getString(int i) {
        return (String) values.get(i);
    }

    public Integer getInteger(int i) {
        return (Integer) values.get(i);
    }

    public Long getLong(int i) {
        return (Long) values.get(i);
    }

    public Boolean getBoolean(int i) {
        return (Boolean) values.get(i);
    }

    public Short getShort(int i) {
        return (Short) values.get(i);
    }

    public Byte getByte(int i) {
        return (Byte) values.get(i);
    }

    public Double getDouble(int i) {
        return (Double) values.get(i);
    }

    public Float getFloat(int i) {
        return (Float) values.get(i);
    }

    public byte[] getBinary(int i) {
        return (byte[]) values.get(i);
    }


    @Override
    public Object getValueByField(String field) {
        return null;
    }

    @Override
    public String getStringByField(String field) {
        return null;
    }

    @Override
    public Integer getIntegerByField(String field) {
        return null;
    }

    @Override
    public Long getLongByField(String field) {
        return null;
    }

    @Override
    public Boolean getBooleanByField(String field) {
        return null;
    }

    @Override
    public Short getShortByField(String field) {
        return null;
    }

    @Override
    public Byte getByteByField(String field) {
        return null;
    }

    @Override
    public Double getDoubleByField(String field) {
        return null;
    }

    @Override
    public Float getFloatByField(String field) {
        return null;
    }

    @Override
    public byte[] getBinaryByField(String field) {
        return new byte[0];
    }

    @Override
    public List<Object> getValues() {
        return values;
    }

    @Override
    public Fields getFields() {
        return null;
    }

    @Override
    public List<Object> select(Fields selector) {
        return values;
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        return null;
    }

    @Override
    public String getSourceComponent() {
        return null;
    }

    @Override
    public int getSourceTask() {
        return 0;
    }

    @Override
    public String getSourceStreamId() {
        return null;
    }

    @Override
    public MessageId getMessageId() {
        return null;
    }
}
