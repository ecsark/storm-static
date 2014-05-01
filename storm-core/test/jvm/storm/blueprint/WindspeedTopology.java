package storm.blueprint;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.LocalCluster;

/**
 * User: ecsark
 * Date: 5/1/14
 * Time: 10:40 AM
 */
public class WindspeedTopology {


    public static void main (String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WindSpeedSpout(), 1);
        builder.setBolt("avg", WindSpeedBoltsReuse.setUpAVGBolts(), 2).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            System.out.println("Using Cluster");
            //conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            //conf.setMaxTaskParallelism(3);
            System.out.println("Using Local");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("windspeed", conf, builder.createTopology());

            Thread.sleep(120000);

            cluster.shutdown();
        }
    }
}
