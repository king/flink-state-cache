package com.king.flink.state;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Copied from org.apache.flink.table.runtime.util.LRUMap to a local
 * file. No need to pull a dependency for such a simple class.
 *
 * An LRU cache, based on <code>LinkedHashMap</code>.
 *
 * <p>This cache has a fixed maximum number of elements (<code>cacheSize</code>).
 * If the cache is full and another entry is added, the LRU (least recently
 * used) entry is dropped.
 *
 * <p>Note: This class is not thread-safe.
 */
public class LRUMap<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 6284793791635376071L;

    private final int cacheSize;
    private final RemovalListener<K, V> removalListener;

    public LRUMap(int cacheSize) {
        this(cacheSize, null);
    }

    public LRUMap(int cacheSize, RemovalListener<K, V> removalListener) {
        super((int) Math.ceil(cacheSize / 0.75) + 1, 0.75F, true);
        this.cacheSize = cacheSize;
        this.removalListener = removalListener;
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        if (size() > cacheSize) {
            if (removalListener != null) {
                removalListener.onRemoval(eldest);
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * An object that can receive a notification when an entry is removed from a LRUMap.
     * @param <K> the type of keys maintained by this map
     * @param <V> the type of mapped values
     */
    public interface RemovalListener<K, V> {
        void onRemoval(Map.Entry<K, V> eldest);
    }
}
