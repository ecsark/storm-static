package storm.blueprint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ecsark
 * Date: 5/15/14
 * Time: 4:27 PM
 */
public class ListMap <K, V> {

    public Map<K, List<V>> map;

    public ListMap () {
        map = new HashMap<K, List<V>>();
    }

    public ListMap (List<V> list, KeyExtractable<K, V> keyExtractable) {
        this();
        load(list, keyExtractable);
    }

    public void load (List<V> list, KeyExtractable<K, V> keyExtractable) {
        for (V item : list) {
            put (keyExtractable.getKey(item), item);
        }
    }

    public void put (K key, V value) {
        if (!map.containsKey(key))
            map.put(key, new ArrayList<V>());

        map.get(key).add(value);
    }

    public interface KeyExtractable <A, B> {
        A getKey (B item);
    }

}
