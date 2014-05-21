package storm.blueprint;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.blueprint.buffer.LibreTupleBuffer;

import java.util.Collection;
import java.util.Map;

/**
 * User: ecsark
 * Date: 5/14/14
 * Time: 3:02 PM
 */
public class LibreBolt extends BaseBasicBolt {

    Collection<LibreTupleBuffer> buffers;

    public void setBoltName(String boltName) {
        this.boltName = boltName;
    }

    public void setOutputFields(Fields outputFields) {
        this.outputFields = outputFields;
    }

    public void setEntrance(LibreEntranceBuffer entrance) {
        this.entrance = entrance;
    }

    String boltName;

    Fields outputFields;

    public BasicOutputCollector getCollector() {
        return collector;
    }

    BasicOutputCollector collector;

    LibreEntranceBuffer entrance;

    Timing timer = new Timing(7000,700);

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        if (entrance == null) {
            throw new RuntimeException("Tuple entrance has not been set!");
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        timer.beforeTest();
        this.collector = collector;
        entrance.put(input);
        timer.afterTest();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (LibreTupleBuffer buf : buffers) {
            declarer.declareStream(buf.getId(), outputFields);
        }
    }
}
