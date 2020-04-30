package com.king.flink.state;

import com.king.flink.metrics.Watch;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public abstract class BaseCachingState<K, V> implements ValueState<V> {
    public final int maxSize;
    protected final AbstractStreamOperator<?> op;
    protected final ValueState<V> state;
    protected final BiFunction<K, V, StateHookResponse<V>> stateGetHook;
    protected final BiFunction<K, V, StateHookResponse<V>> stateUpdateHook;
    // Metric trackers
    protected final Consumer<Boolean> cacheHitTracker;
    protected final Watch getWatch;
    protected final Watch writeWatch;
    protected LRUMap<K, CacheEntry> values;

    public BaseCachingState(AbstractStreamOperator<?> op, ValueState<V> state, BiFunction<K, V, StateHookResponse<V>> stateGetHook, BiFunction<K, V, StateHookResponse<V>> stateUpdateHook, Integer maxSize, Consumer<Boolean> cacheHitTracker, Watch readWatch, Watch writeWatch) {
        if (maxSize == null || maxSize < 1) {
            throw new IllegalArgumentException("Cache size needs to be at least 1");
        }
        if (state == null) {
            throw new IllegalArgumentException("state not specified");
        }
        if (op == null) {
            throw new IllegalArgumentException("op not specified");
        }

        this.op = op;
        this.state = state;
        this.stateGetHook = stateGetHook;
        this.stateUpdateHook = stateUpdateHook;
        this.maxSize = maxSize;
        this.cacheHitTracker = cacheHitTracker;
        this.getWatch = readWatch;
        this.writeWatch = writeWatch;
    }

    /**
     * Writes all the dirty entries in the cache to the underlying state and marks
     * them clean.
     *
     * @throws IOException Thrown if the system cannot access the state.
     */
    public void updateAllStates() throws IOException {
        preserveKey(() -> {
            Iterator<Map.Entry<K, CacheEntry>> iterator = values.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<K, CacheEntry> mapEntry = iterator.next();
                K key = mapEntry.getKey();
                CacheEntry value = mapEntry.getValue();

                if (!value.isDirty()) {
                    continue;
                }
                DirtyCacheEntry dirtyCacheEntry = (DirtyCacheEntry) value;

                op.setCurrentKey(key);
                updateState(key, dirtyCacheEntry);
                mapEntry.setValue(new CleanCacheEntry(dirtyCacheEntry.getValue()));
            }
        });
    }

    protected void updateState(K key, CacheEntry<V> cacheEntry) throws IOException {
        // Sanity check
        if (!cacheEntry.isDirty()) {
            throw new IllegalStateException("Trying to update a clean entry");
        }

        V val = cacheEntry.getValue();

        if (stateUpdateHook != null) {
            StateHookResponse<V> response = stateUpdateHook.apply(key, val);
            if (response == null) {
                throw new NullPointerException("stateUpdateHook returned null");
            }

            if (response == StateHookResponse.IDENTITY) {
            } else if (response instanceof StateHookResponse.MutateStateHookResponse) {
                val = ((StateHookResponse.MutateStateHookResponse<V>) response).getValue();
                ((DirtyCacheEntry<V>) cacheEntry).setValue(val);
            } else {
                throw new IllegalStateException(response.getClass().toString());
            }
        }

        if (writeWatch != null) {
            writeWatch.restart();
        }
        if (val != null) {
            state.update(val);
        } else {
            state.clear();
        }
        if (writeWatch != null) {
            writeWatch.stopAndRecord();
        }
    }


    /**
     * Runs the given operation {@code r}. Before the
     * operations stores the key of the operator and
     * after running to operations sets back the key.
     *
     * @param r the operation to run
     * @throws IOException the exception from the operation
     */
    protected void preserveKey(RunnableWithIOException r) throws IOException {
        K prevKey = (K) op.getCurrentKey();
        try {
            r.run();
        } finally {
            if (prevKey != null) {
                op.setCurrentKey(prevKey);
            }
        }
    }

    /**
     * For testing.
     */
    Map<K, CacheEntry> getValues() {
        return new HashMap<>(values);
    }

    protected interface RunnableWithIOException {
        void run() throws IOException;
    }

    /**
     * Entry for the cache. Using subclasses instead of storing
     * dirtiness in e.g. a boolean. Saves some space, since the
     * class indicates the dirtiness.
     *
     * @param <V> the cache value type
     */
    protected abstract static class CacheEntry<V> {
        protected V value;

        public CacheEntry(V value) {
            this.value = value;
        }

        public abstract boolean isDirty();

        public V getValue() {
            return value;
        }
    }

    protected static class CleanCacheEntry<V> extends CacheEntry<V> {
        public CleanCacheEntry(V value) {
            super(value);
        }

        @Override
        public boolean isDirty() {
            return false;
        }

        @Override
        public String toString() {
            return "CleanCacheEntry{" +
                    "value=" + value +
                    '}';
        }
    }

    protected static class DirtyCacheEntry<V> extends CacheEntry<V> {
        public DirtyCacheEntry(V value) {
            super(value);
        }

        @Override
        public boolean isDirty() {
            return true;
        }

        public void setValue(V value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "CleanCacheEntry{" +
                    "value=" + value +
                    '}';
        }
    }
}
