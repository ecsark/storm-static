package storm.blueprint.buffer;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;

/**
 * User: ecsark
 * Date: 5/14/14
 * Time: 3:13 PM
 */
public interface WindowResultCallback extends Serializable {

    void process (Tuple tuple);

}
