package storm.blueprint;

import backtype.storm.tuple.Fields;
import storm.blueprint.buffer.TupleBuffer;
import storm.blueprint.function.Functional;

import java.io.Serializable;
import java.util.*;

/**
 * User: ecsark
 * Date: 6/1/14
 * Time: 11:32 PM
 */
public abstract class AutoBoltBuilder implements Serializable {

    Functional function;

    AutoBolt bolt;

    Fields inputFields;

    transient Set<String> windowNames;

    transient List<TupleBuffer> buffers;

    transient Random rand;

    transient int cells = 0;

    public AutoBoltBuilder() {
        windowNames = new HashSet<String>();
        bolt = new AutoBolt();
        rand = new Random();
        buffers = new ArrayList<TupleBuffer>();
        bolt.buffers = buffers;
    }

    public AutoBoltBuilder setInputFields(Fields inputFields) {
        this.inputFields = inputFields;
        return this;
    }

    public AutoBoltBuilder setOutputFields(Fields outputFields) {
        bolt.setOutputFields(outputFields);
        return this;
    }

    public AutoBoltBuilder setFunction(Functional function) {
        this.function = function;
        return this;
    }

    public abstract AutoBoltBuilder addWindow(String id, int length, int pace);

    public abstract AutoBolt build();

    protected String uniqueWindowName (String windowName) {
        //make sure there are no id duplicates!
        while (windowNames.contains(windowName)) {
            windowName += "_" + rand.nextInt(100);
        }
        windowNames.add(windowName);

        return windowName;
    }

    public int getTotalCells () {
        return cells;
    }
}
