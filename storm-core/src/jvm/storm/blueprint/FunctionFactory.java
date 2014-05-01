package storm.blueprint;

import storm.blueprint.function.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 6:11 PM
 */
public class FunctionFactory implements Serializable{

    Map<String, Class<? extends Functional>> registry;

    public FunctionFactory () {
        registry = new HashMap<String, Class<? extends Functional>>();
        registerFunction();
    }

    /*
    Since we are not using Spring, we have to do
    the registration manually.
     */
    private void registerFunction () {
        registry.put("avg", Average.class);
        registry.put("rms", RootMeanSquare.class);
        registry.put("dev", StandardDeviation.class);
        registry.put("max", Max.class);
    }

    public Functional getFunction(String name) throws FunctionNotSupportedException {
        if (!registry.containsKey(name.toLowerCase())) {
            throw new FunctionNotSupportedException("Function with name " + name + " not found!");
        }
        try {
            return registry.get(name.toLowerCase()).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
