package storm.blueprint;

import java.util.List;
import java.util.Random;

/**
 * User: ecsark
 * Date: 4/30/14
 * Time: 10:46 PM
 */
public class WindSpeedBoltsPro {

    static AutoBoltPro setUpAVGBolts() {
        AutoBoltBuilderPro builder = new AutoBoltBuilderPro("windspeed_avg");
        try {
            AutoBoltPro bolt = builder
                    .setOutputFields("windspeed_avg")
                    .setInputFields("windspeed")
                    .addTupleBuffer("wsavg_3s", 3, 1, "avg", "incr") //refreshed every 1s
                    .setEntrance("wsavg_3s")
                    .addTupleBuffer("wsavg_15s", 5, 1, "avg", "incr") //refreshed every 3s
                    .pushToStack("wsavg_3s","wsavg_15s", 3)
                    .addTupleBuffer("wsavg_1m", 20, 1, "avg", "incr") //refreshed every 3s
                    .pushToStack("wsavg_3s","wsavg_1m", 3)
                    .addTupleBuffer("wsavg_10m", 200, 5, "avg", "incr") //refreshed every 15s
                    .pushToStack("wsavg_3s", "wsavg_10m",3)
                    .addTupleBuffer("wsavg_60m", 60, 1, "avg", "incr") //refreshed every 1m
                    .pushToStack("wsavg_1m", "wsavg_60m", 20)
                    .build();
            return bolt;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static AutoBoltPro setUpSUMBolts() {
        AutoBoltBuilderPro builder = new AutoBoltBuilderPro("windspeed_sum");
        try {
            AutoBoltPro bolt = builder
                    .setOutputFields("windspeed_sum")
                    .setInputFields("windspeed")

                    .addTupleBuffer("wsmax_5s", 5, 5, "sum", "full") //refreshed every 5s
                    .setEntrance("wsmax_5s")

                    .addTupleBuffer("wsmax_55s", 11, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_55s", 1)

                    .addTupleBuffer("wsmax_75s", 15, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_75s",1)

                    .addTupleBuffer("wsmax_200s", 40, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_200s", 1)

                    .addTupleBuffer("wsmax_360s", 72, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_360s", 1)

                    .addTupleBuffer("wsmax_400s", 80, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_400s", 1)

                    .addTupleBuffer("wsmax_450s", 90, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_450s", 1)

                    .addTupleBuffer("wsmax_480s", 96, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_480s", 1)

                    .addTupleBuffer("wsmax_500s", 100, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_500s", 1)

                    .addTupleBuffer("wsmax_520s", 104, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_520s", 1)

                    .addTupleBuffer("wsmax_540s", 108, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_540s", 1)

                    .addTupleBuffer("wsmax_550s", 110, 1, "sum", "full") //refreshed every 5s
                    .pushToStack("wsmax_5s", "wsmax_550s", 1)

                    .build();
            return bolt;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    static AutoBoltPro setupAutoGeneratingBolt() {
        AutoBoltBuilderPro builder = new AutoBoltBuilderPro("windspeed_sum");

        try {

            String baseTupleBufferName = "wssum_5s";

            builder.setOutputFields("windspeed_sum")
                    .setInputFields("windspeed")
                    .addTupleBuffer("wssum_5s", 5, 5, "sum", "full") //refreshed every 5s
                    .setEntrance(baseTupleBufferName);

            List<Integer> res = QueryGenerator.generate(5467, 100, 200);

            Random rand = new Random();
            for (int r : res) {
                String tupleBufferName = "wssum_" + r*5 + "s_" + rand.nextLong();
                builder.addTupleBuffer(tupleBufferName, r, 1, "sum", "full")
                        .pushToStack(baseTupleBufferName, tupleBufferName, 1);
            }

            return builder.build();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
