package storm.blueprint;

/**
 * User: ecsark
 * Date: 4/30/14
 * Time: 10:46 PM
 */
public class WindSpeedBoltsReuse {

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
}
