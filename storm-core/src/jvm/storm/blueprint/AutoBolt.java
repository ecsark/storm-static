package storm.blueprint;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.blueprint.buffer.IEntrance;
import storm.blueprint.buffer.TupleBuffer;

import java.util.Collection;
import java.util.Map;

/**
 * User: ecsark
 * Date: 5/14/14
 * Time: 3:02 PM
 */
public class AutoBolt extends BaseBasicBolt {

    Collection<TupleBuffer> buffers;

    public void setOutputFields(Fields outputFields) {
        this.outputFields = outputFields;
    }

    public void setEntrance(IEntrance entrance) {
        this.entrance = entrance;
    }


    Fields outputFields;

    public BasicOutputCollector getCollector() {
        return collector;
    }

    BasicOutputCollector collector;

    IEntrance entrance;

    Timing timer = new Timing(7000,700);

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        if (entrance == null) {
            throw new RuntimeException("Tuple entrance has not been set!");
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        timer.beforeTest(); //TODO: remove timing
        this.collector = collector;
        entrance.put(input);
        timer.afterTest(); //TODO: remove timing
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (TupleBuffer buf : buffers) {
            if (buf.isEmitting())
                declarer.declareStream(buf.getId(), outputFields);
        }
    }
}
