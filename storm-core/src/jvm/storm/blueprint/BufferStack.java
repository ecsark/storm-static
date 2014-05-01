package storm.blueprint;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.blueprint.buffer.FakeTuple;
import storm.blueprint.buffer.TupleBuffer;
import storm.blueprint.function.Functional;

import java.io.Serializable;
import java.util.List;

/**
 * User: ecsark
 * Date: 4/28/14
 * Time: 2:32 PM
 */
public class BufferStack implements Serializable {

    TupleBuffer buffer;

    String outputStreamId;

    Functional function;

    Values state;

    List<BufferCounter> chainedBuffers;

    void addListenerBuffer (TupleBuffer listenerBuffer, int pace) {
        BufferCounter bc = new BufferCounter();
        bc.buffer = listenerBuffer;
        bc.pace = pace;
        bc.counter = 0;
        chainedBuffers.add(bc);
    }

    void propagate (Values values) {
        for (BufferCounter bc : chainedBuffers) {
            bc.checkAndPut(values);
        }
    }

    class BufferCounter implements Serializable {
        TupleBuffer buffer;
        int pace;
        int counter;

        BufferCounter () {}

        boolean checkAndPut (Values values) {
            counter = (counter+1) % pace;
            if (counter == 0) {
                Tuple result = new FakeTuple(values);
                buffer.put(result);
                return true;
            }
            return false;
        }
    }
}
