package storm.blueprint.buffer;

import backtype.storm.tuple.Fields;
import storm.blueprint.function.Functional;

import java.io.Serializable;

/**
 * User: ecsark
 * Date: 6/1/14
 * Time: 5:52 PM
 */
public abstract class TupleBuffer implements Serializable {

    String id;
    int pace;
    int size; // number of cells in the window, eg. [(1,2,3),(4,5),(6,7)] ->3
    int length;  // actual window coverage, eg. [1,2,3,4,5,6,7] ->7

    Fields selectFields;
    Functional function;

    boolean emitting = true;

    public void setEmitting (boolean emitting) {
        this.emitting = emitting;
    }

    public void setSelectFields (Fields selectFields) {
        this.selectFields = selectFields;
    }

    public void setFunction(Functional function) {
        this.function = function;
    }

    public String getId() {
        return id;
    }

    public int getSize () {
        return size;
    }

    public int getPace () {
        return pace;
    }

    public int getLength () { return length;}

    public boolean isEmitting() {
        return emitting;
    }

}
