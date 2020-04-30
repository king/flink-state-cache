package com.king.flink.state;

import com.king.flink.metrics.Watch;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * A {@link ValueState} that uses a {@link LRUMap} to cache state values in the heap.
 *
 * @param <K> Type of the key of the keyed state backend of {@link AbstractStreamOperator}.
 * @param <V> Type of the value in the state.
 */
public class CachingState<K, V> extends BaseCachingState<K, V> {

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	private CachingState(Integer maxSize, ValueState<V> state, AbstractStreamOperator<?> op,
						 BiFunction<K, V, StateHookResponse<V>> stateGetHook, BiFunction<K, V, StateHookResponse<V>> stateUpdateHook,
						 Consumer<Boolean> cacheHitTracker, Watch readWatch, Watch writeWatch) {
		super(op, state, stateGetHook, stateUpdateHook, maxSize, cacheHitTracker, readWatch, writeWatch);
		if (maxSize == null || maxSize < 1) {
			throw new IllegalArgumentException("Cache size needs to be at least 1");
		}

		if (state == null) {
			throw new IllegalArgumentException("No state specified");
		}

		if (op == null) {
			throw new IllegalArgumentException("No op specified");
		}
		KeyedStateBackend<K> keyedStateBackend = (KeyedStateBackend<K>) op.getKeyedStateBackend();
		if (keyedStateBackend == null) {
			throw new IllegalArgumentException("The given AbstractStreamOperator doesn't have keyed state backend");
		}

		this.values = new LRUMap<>(maxSize, (mapEntry) -> {
			CacheEntry cacheEntry = mapEntry.getValue();
			if (!cacheEntry.isDirty()) {
				// No need to write, hasn't been updated.
				return;
			}

			try {
				preserveKey(() -> {
					K key = mapEntry.getKey();
					op.setCurrentKey(key);
					updateState(key, cacheEntry);
				});
			} catch (IOException e) {
				// Not the best way, better would be to be able to expose the IOException
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void clear() {
		values.remove((K) op.getCurrentKey());
		state.clear();
	}

	@Override
	public void update(V value) throws IOException {
		// TODO: will this throws IOException if put throws?
		values.put((K) op.getCurrentKey(), new DirtyCacheEntry(value));
	}

	@Override
	public V value() throws IOException {
		return get((K) op.getCurrentKey());
	}

	@Override
	public String toString() {
		return "CachingState [values=" + values.toString() + "]";
	}

	private V get(K key) throws IOException {
		CacheEntry<V> cacheEntry = values.get(key);
		V val = cacheEntry == null ? null : cacheEntry.getValue();

		if (val == null) {
			if (cacheHitTracker != null) {
				cacheHitTracker.accept(false);
			}
			if (getWatch != null) {
				getWatch.restart();
			}
			val = state.value();
			if (getWatch != null) {
				getWatch.stopAndRecord();
			}

			if (stateGetHook != null) {
				StateHookResponse<V> response = stateGetHook.apply(key, val);
				if (response == null) {
					throw new NullPointerException("stateGetHook returned null");
				}

				if (response == StateHookResponse.IDENTITY) {
					cacheEntry = new CleanCacheEntry(val);
				} else if (response instanceof StateHookResponse.MutateStateHookResponse) {
					val = ((StateHookResponse.MutateStateHookResponse<V>) response).getValue();
					cacheEntry = new DirtyCacheEntry(val);
				} else {
					throw new IllegalStateException(response.getClass().toString());
				}
			} else {
				cacheEntry = new CleanCacheEntry<>(val);
			}

			values.put((K) op.getCurrentKey(), cacheEntry);
		} else {
			if (cacheHitTracker != null) {
				cacheHitTracker.accept(true);
			}
		}
		return val;
	}

	public static class Builder<K, V> {
		Integer maxSize;
		ValueState<V> state;
		AbstractStreamOperator<?> operator;
		BiFunction<K, V, StateHookResponse<V>> stateGetHook;
		BiFunction<K, V, StateHookResponse<V>> stateUpdateHook;
		Consumer<Boolean> cacheHitTracker;
		Watch readWatch;
		Watch writeWatch;

		/**
		 * Sets the maximum number of entries in the cache.
		 *
		 * @param maxSize the size
		 * @return this
		 */
		public Builder<K, V> maxSize(Integer maxSize) {
			this.maxSize = maxSize;
			return this;
		}

		/**
		 * The {@link ValueState} to wrap with cache.
		 *
		 * @param state the state
		 * @return this
		 */
		public Builder<K, V> state(ValueState<V> state) {
			this.state = state;
			return this;
		}

		/**
		 * The operator. Needed to be able to access the keyed state backend.
		 *
		 * @param operator the operator
		 * @return this
		 */
		public Builder<K, V> operator(AbstractStreamOperator<?> operator) {
			this.operator = operator;
			return this;
		}

		/**
		 * A function to apply to a cache value after the value has been
		 * fetched from the wrapped to the cache, i.e. {@link ValueState#value()}-call.
		 * Can be used e.g. to clean up the state or metrics purposes.
		 *
		 * @param stateGetHook the hook
		 * @return this
		 */
		public Builder<K, V> stateGetHook(BiFunction<K, V, StateHookResponse<V>> stateGetHook) {
			this.stateGetHook = stateGetHook;
			return this;
		}

		/**
		 * A function to apply to a cache value before it is
		 * written from the cache to the underlying {@link ValueState},
		 * i.e. {@link ValueState#update(Object)}-call.
		 * Can be used e.g. to clean up the state or metrics purposes.
		 *
		 * @param stateUpdateHook the hook
		 * @return this
		 */
		public Builder<K, V> stateUpdateHook(BiFunction<K, V, StateHookResponse<V>> stateUpdateHook) {
			this.stateUpdateHook = stateUpdateHook;
			return this;
		}

		/**
		 * Called with {@code true} when getting a values from the cache
		 * and key was found in the cache (cache hit) and {@code false}
		 * if the the key was not found in the cache (cache miss).
		 *
		 * @param cacheHitTracker the tracker
		 * @return this
		 */
		public Builder<K, V> cacheHitTracker(Consumer<Boolean> cacheHitTracker) {
			this.cacheHitTracker = cacheHitTracker;
			return this;
		}

		/**
		 * Sets a {@link Watch} to track the duration of {@link ValueState#value()}-call
		 * when getting a value from the cache and there's cache miss.
		 *
		 * @param readWatch the watch
		 * @return this
		 */
		public Builder<K, V> readWatch(Watch readWatch) {
			this.readWatch = readWatch;
			return this;
		}

		/**
		 * Sets a {@link Watch} to track the duration of {@link ValueState#update(Object)}-call
		 * when flushing a value from the cache to the underlying {@link ValueState}.
		 *
		 * @param writeWatch the watch
		 * @return this
		 */
		public Builder<K, V> writeWatch(Watch writeWatch) {
			this.writeWatch = writeWatch;
			return this;
		}

		/**
		 * Builds.
		 *
		 * @return the CachingState
		 */
		public CachingState<K, V> build() {
			return new CachingState<>(maxSize, state, operator, stateGetHook, stateUpdateHook, cacheHitTracker, readWatch, writeWatch);
		}
	}
}
