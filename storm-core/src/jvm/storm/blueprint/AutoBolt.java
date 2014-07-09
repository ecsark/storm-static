package storm.blueprint;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.blueprint.buffer.IEntrance;
import storm.blueprint.buffer.TupleBuffer;
import storm.blueprint.util.Timer;

import java.util.List;
import java.util.Map;

/**
 * User: ecsark
 * Date: 5/14/14
 * Time: 3:02 PM
 */
public class AutoBolt extends BaseBasicBolt {

    List<TupleBuffer> buffers;

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

    Timer timer;

    AutoBolt setTimer (Timer timer) {
        this.timer = timer;
        return this;
    }

    boolean firstRun = true;
    String id;

    public String getId() {
        return id;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        if (entrance == null) {
            throw new RuntimeException("Tuple entrance has not been set!");
        }
        id = context.getThisComponentId();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (firstRun) {
            this.collector = collector;
            if (timer!=null)
                timer.beforeTest(); //TODO: remove timing
        }

        entrance.put(input);

        if (timer!=null)
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
