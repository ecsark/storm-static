package storm.blueprint;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * User: ecsark
 * Date: 5/1/14
 * Time: 10:32 AM
 */
public class WindSpeedSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    Random _rand;
    double value;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("windspeed"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        value = 300.0;
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void nextTuple() {
        Utils.sleep(20);
        //if (_rand.nextBoolean())
          //  value += _rand.nextDouble()*2;
        //else
            value -= _rand.nextDouble()*2;
        _collector.emit(new Values(value));
    }
}
