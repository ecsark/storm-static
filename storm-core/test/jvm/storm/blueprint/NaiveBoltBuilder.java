package storm.blueprint;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.blueprint.buffer.FullWindowBuffer;
import storm.blueprint.buffer.IEntrance;
import storm.blueprint.buffer.TupleBuffer;
import storm.blueprint.buffer.WindowResultCallback;

/**
 * User: ecsark
 * Date: 6/4/14
 * Time: 9:53 AM
 */
public class NaiveBoltBuilder extends AutoBoltBuilder {

    @Override
    public AutoBoltBuilder addWindow(String id, int length, int pace) {
        final String windowName = uniqueWindowName(id);
        final FullWindowBuffer buffer = new FullWindowBuffer(windowName, length, pace, length);
        buffer.setSelectFields(inputFields);
        buffer.setFunction(function);
        buffer.addCallback(new WindowResultCallback() {
            @Override
            public void process(Tuple tuple) {
                bolt.getCollector().emit(buffer.getId(), new Values(tuple.getValues().get(0)));
                //ResultWriter.write(bolt.getId(), windowName + ": " + tuple.getValues().get(0) + "\n");

            }
        });
        buffers.add(buffer);

        return this;
    }

    @Override
    public AutoBolt build() {
        bolt.setEntrance(new IEntrance() {
            @Override
            public void put(Tuple tuple) {
                for (TupleBuffer buffer : bolt.buffers) {
                    ((IEntrance)buffer).put(tuple);
                }
            }
        });

        return bolt;
    }
}
