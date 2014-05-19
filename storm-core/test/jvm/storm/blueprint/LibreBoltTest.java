package storm.blueprint;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.blueprint.function.Max;

public class LibreBoltTest {

    static LibreBolt setupLibreBolt () {
        LibreBoltBuilder builder = new LibreBoltBuilder("avg");
        return builder.setFunction(new Max())
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_avg"))
                .addWindow("28/5",28,5)
                .addWindow("26/5",26,5)
                .addWindow("43/10",43,10)
                .addWindow("8/5",8,5)
                .addWindow("12/5",12,5)
                .build();
    }

    public static void main (String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WindSpeedSpout(), 1);
        builder.setBolt("avg", setupLibreBolt(), 1).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);

        ILocalCluster cluster = new LocalCluster();
        cluster.submitTopology("windspeed", conf, builder.createTopology());

        Thread.sleep(60000);

        cluster.shutdown();

    }

}