package storm.blueprint;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.blueprint.function.Max;
import storm.blueprint.function.Sum;

import java.util.List;
import java.util.Random;

public class LibreBoltTest {

    static LibreBolt setupLibreBolt () {
        LibreBoltBuilder builder = new LibreBoltBuilder("max");
        return builder.setFunction(new Max())
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_max"))
                .addWindow("28/5",28,5)
                .addWindow("26/5",26,5)
                .addWindow("43/10",43,10)
                .addWindow("8/5",8,5)
                .addWindow("12/5",12,5)
                .addWindow("60/5",60,5)
                .addWindow("120/20",120,20)
                .build();
    }


    static LibreBolt setupIssueBolt () {
        LibreBoltBuilder builder = new LibreBoltBuilder("max");
        return builder.setFunction(new Sum())
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_max"))
                .addWindow("5/5", 5, 5)
                .addWindow("40/5", 40, 5)
                .addWindow("90/5",90,5)
                .build();
    }

    static LibreBolt setupComparisonBolt () {
        LibreBoltBuilder builder = new LibreBoltBuilder("max");
        return builder.setFunction(new Sum())
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_max"))
                .addWindow("5/5", 5, 5)
                .addWindow("55/5", 55, 5)
                .addWindow("75/5",75,5)
                .addWindow("200/5",200,5)
                .addWindow("360/5", 360, 5)
                .addWindow("400/5", 400, 5)
                .addWindow("450/5", 450, 5)
                .addWindow("480/5",480,5)
                .addWindow("500/5", 500, 5)
                .addWindow("520/5", 500, 5)
                .addWindow("540/5", 540, 5)
                .addWindow("550/5", 500, 5)
                .build();
    }


    static LibreBolt setupAutoGeneratingBolt () {
        LibreBoltBuilder builder = new LibreBoltBuilder("sum");
        builder.setFunction(new Sum())
                .setInputFields(new Fields("windspeed"))
                .setOutputFields(new Fields("windspeed_sum"));

        List<Integer> res = QueryGenerator.generate(5467, 100, 200);
        Random rand = new Random();
        for (int r : res) {
            builder.addWindow(r*5+"/5", r*5, 5);
        }

        return builder.build();
    }

    public static void main (String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WindSpeedSpout(), 1);
        //builder.setBolt("max", setupLibreBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("sum", setupAutoGeneratingBolt(), 1).shuffleGrouping("spout");
        //builder.setBolt("sum", setupIssueBolt(), 1).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(false);

        ILocalCluster cluster = new LocalCluster();
        cluster.submitTopology("windspeed", conf, builder.createTopology());

        Thread.sleep(180000);

        cluster.shutdown();

    }

}