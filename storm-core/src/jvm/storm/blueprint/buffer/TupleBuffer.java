package storm.blueprint.buffer;

import backtype.storm.tuple.Tuple;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 6:40 PM
 */
public abstract class TupleBuffer {

    public abstract void put(Tuple tuple);

}
