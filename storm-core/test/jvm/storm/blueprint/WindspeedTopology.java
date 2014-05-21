package storm.blueprint;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * User: ecsark
 * Date: 5/1/14
 * Time: 10:40 AM
 */
public class WindspeedTopology {


    public static void main (String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WindSpeedSpout(), 1);
        //builder.setBolt("avg", WindSpeedBoltsPro.setUpAVGBolts(), 1).shuffleGrouping("spout");
        builder.setBolt("sum", WindSpeedBoltsPro.setupAutoGeneratingBolt(), 1).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            System.out.println("Using Cluster");
            //conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            //conf.setMaxTaskParallelism(3);
            System.out.println("Using Local");
            ILocalCluster cluster = new LocalCluster();
            cluster.submitTopology("windspeed", conf, builder.createTopology());

            Thread.sleep(180000);

            cluster.shutdown();
        }
    }
}
