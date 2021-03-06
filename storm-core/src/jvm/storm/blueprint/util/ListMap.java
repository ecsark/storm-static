package storm.blueprint.util;

import java.io.Serializable;
import java.util.*;

/**
 * User: ecsark
 * Date: 5/15/14
 * Time: 4:27 PM
 */
public class ListMap <K, V> implements Serializable {

    public Map<K, List<V>> map;

    public ListMap () {
        map = new HashMap<K, List<V>>();
    }

    public ListMap (Collection<V> collection, KeyExtractable<K, V> keyExtractable) {
        this();
        load(collection, keyExtractable);
    }

    public void load (Collection<V> collection, KeyExtractable<K, V> keyExtractable) {
        for (V item : collection) {
            put (keyExtractable.getKey(item), item);
        }
    }

    public void put (K key, V value) {
        touch(key);
        map.get(key).add(value);
    }

    public void putUnique (K key, V value) {
        touch(key);
        if (!map.get(key).contains(value))
            map.get(key).add(value);
    }

    public void touch (K key) {
        if (!map.containsKey(key))
            map.put(key, new ArrayList<V>());
    }

    public interface KeyExtractable <A, B> {
        A getKey (B item);
    }

    public Map<K, List<V>> getMap () {
        return map;
    }

    public List<V> get (K key) {
        return map.get(key);
    }

    public Set<Map.Entry<K, List<V>>> entrySet() {
        return map.entrySet();
    }

    public List<V> values () {
        List<V> vals = new ArrayList<V>();
        for (List<V> v : map.values()) {
            vals.addAll(v);
        }
        return vals;
    }

    public Set<K> keySet () {
        return map.keySet();
    }

    public boolean containsKey (K key) {
        return map.containsKey(key);
    }
}
