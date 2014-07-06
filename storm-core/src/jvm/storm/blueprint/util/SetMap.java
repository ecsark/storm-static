package storm.blueprint.util;

import java.io.Serializable;
import java.util.*;

/**
 * User: ecsark
 * Date: 5/15/14
 * Time: 4:27 PM
 */
public class SetMap<K, V> implements Serializable {

    public Map<K, Set<V>> map;

    public SetMap() {
        map = new HashMap<K, Set<V>>();
    }

    public SetMap(Collection<V> collection, KeyExtractable<K, V> keyExtractable) {
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

    public void touch (K key) {
        if (!map.containsKey(key))
            map.put(key, new HashSet<V>());
    }

    public interface KeyExtractable <A, B> {
        A getKey(B item);
    }

    public Map<K, Set<V>> getMap () {
        return map;
    }

    public Set<V> get (K key) {
        return map.get(key);
    }

    public Set<Map.Entry<K, Set<V>>> entrySet() {
        return map.entrySet();
    }

    public List<V> values () {
        List<V> vals = new ArrayList<V>();
        for (Set<V> v : map.values()) {
            vals.addAll(v);
        }
        return vals;
    }

    public Set<K> keysSet () {
        return map.keySet();
    }

    public boolean containsKey (K key) {
        return map.containsKey(key);
    }
}
