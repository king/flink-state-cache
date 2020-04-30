package com.king.flink.state;

import com.king.flink.metrics.Watch;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * A {@link ValueState} that uses a {@link LRUMap} to cache state values in the heap.
 *
 * @param <K> Type of the key of the keyed state backend of {@link AbstractStreamOperator}.
 * @param <V> Type of the value in the state.
 */
public class CachingStateL2<K, V> extends BaseCachingState<K, V> {
	/**
	 * Write-through LRU cache.
	 */
	private final LRUMap<K, CacheEntry> valuesL2;

	public final int maxSizeL2;
	private final Consumer<Boolean> cacheHitTrackerL2;

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	private CachingStateL2(Integer maxSize, Integer maxSizeL2, ValueState<V> state, AbstractStreamOperator<?> op,
						   BiFunction<K, V, StateHookResponse<V>> stateGetHook, BiFunction<K, V, StateHookResponse<V>> stateUpdateHook,
						   Consumer<Boolean> cacheHitTracker, Consumer<Boolean> cacheHitTrackerL2, Watch readWatch, Watch writeWatch) {
		super(op, state, stateGetHook, stateUpdateHook, maxSize, cacheHitTracker, readWatch, writeWatch);
		if (maxSizeL2 == null || maxSizeL2 < 1) {
			throw new IllegalArgumentException("Cache size needs to be at least 1");
		}
		this.maxSizeL2 = maxSizeL2;

		this.cacheHitTrackerL2 = cacheHitTrackerL2;

		this.valuesL2 = new LRUMap<>(maxSizeL2, null);
		this.values = new LRUMap<>(maxSize,
				(mapEntry) -> {
					K key = mapEntry.getKey();
					CacheEntry cacheEntry = mapEntry.getValue();
					// Write-through. L2 will only have clean entries.
					CacheEntry cleanCacheEntry;
					if (cacheEntry.isDirty()) {
						try {
							preserveKey(() -> {
								this.op.setCurrentKey(key);
								updateState(key, cacheEntry);
							});
						} catch (IOException e) {
							// Not the best way, better would be to be able to expose the IOException
							throw new RuntimeException(e);
						}
						cleanCacheEntry = new CleanCacheEntry(cacheEntry.getValue());
					} else {
						cleanCacheEntry = cacheEntry;
					}

					valuesL2.put(key, cleanCacheEntry);
				});
	}

	@Override
	public void clear() {
		K key = (K) op.getCurrentKey();
		if (values.remove(key) == null) {
			// Wasn't in l1, can be in l2
			valuesL2.remove(key);
		}
		state.clear();
	}

	@Override
	public void update(V value) throws IOException {
		// TODO: will this throws IOException if put throws?
		K key = (K) op.getCurrentKey();

		if (values.containsKey(key)) {
			// Will fit to l1, because existing value fits
		} else {
			// Promote to l1 if in l2, or just put to l1
			// Might trigger a l1 -> l2 for some other entry in l1
			valuesL2.remove(key);
		}

		// Always to l1, because this entry is the most least used
		values.put(key, new DirtyCacheEntry(value));
	}

	@Override
	public V value() throws IOException {
		return get((K) op.getCurrentKey());
	}

	@Override
	public String toString() {
		return "CachingStateL2{" +
				"values=" + values.size() +
				", valuesL2=" + valuesL2.size() +
				'}';
	}

	/**
	 * For testing.
	 */
	Map<K, CacheEntry> getValuesL2() {
		return new HashMap<>(valuesL2);
	}

	public int sizeL1() {
		return values.size();
	}

	public int sizeL2() {
		return valuesL2.size();
	}

	public int size() {
		return sizeL1() + sizeL2();
	}

	private V get(K key) throws IOException {
		CacheEntry<V> cacheEntry = values.get(key);
		V val = cacheEntry == null ? null : cacheEntry.getValue();

		if (val == null) {
			if (cacheHitTracker != null) {
				cacheHitTracker.accept(false);
			}

			cacheEntry = valuesL2.get(key);
			if (cacheEntry != null) {
				// In L2. Promote to L1, might evict another entry from L1 to L2
				if (cacheHitTrackerL2 != null) {
					cacheHitTrackerL2.accept(true);
				}
				val = cacheEntry.getValue();
				valuesL2.remove(key);
				values.put(key, cacheEntry);
			} else {
				// Not in L2, get from underlying state
				if (cacheHitTrackerL2 != null) {
					cacheHitTrackerL2.accept(false);
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
						cacheEntry = new DirtyCacheEntry<>(val);
					} else {
						throw new IllegalStateException(response.getClass().toString());
					}
				} else {
					cacheEntry = new CleanCacheEntry<>(val);
				}

				values.put(key, cacheEntry);
			}
		} else {
			if (cacheHitTracker != null) {
				cacheHitTracker.accept(true);
			}
		}
		return val;
	}

	public static class Builder<K, V> {
		Integer maxSize;
		Integer maxSizeL2;
		ValueState<V> state;
		AbstractStreamOperator<?> operator;
		BiFunction<K, V, StateHookResponse<V>> stateGetHook;
		BiFunction<K, V, StateHookResponse<V>> stateUpdateHook;
		Consumer<Boolean> cacheHitTracker;
		Consumer<Boolean> cacheHitTrackerL2;
		Watch readWatch;
		Watch writeWatch;

		/**
		 * Sets the maximum number of entries in the L1 layer of the cache.
		 *
		 * @param maxSize l1 size
		 * @return this
		 */
		public Builder<K, V> maxSize(Integer maxSize) {
			this.maxSize = maxSize;
			return this;
		}

		/**
		 * Sets the maximum number of entries in the L2 layer of the cache.
		 *
		 * @param maxSizeL2 the size
		 * @return this
		 */
		public Builder<K, V> maxSizeL2(Integer maxSizeL2) {
			this.maxSizeL2 = maxSizeL2;
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
		 * and key was found in the L1 cache (cache hit) and {@code false}
		 * if the the key was not found in the L1 cache (L1 cache miss).
		 *
		 * @param cacheHitTracker the tracker
		 * @return this
		 */
		public Builder<K, V> cacheHitTracker(Consumer<Boolean> cacheHitTracker) {
			this.cacheHitTracker = cacheHitTracker;
			return this;
		}

		/**
		 * Called with {@code true} when getting a values from the cache
		 * and key was found in the L2 cache (L2 cache hit) and {@code false}
		 * if the the key was not found in the L2 cache (L2 cache miss).
		 *
		 * @param cacheHitTrackerL2 the tracker
		 * @return this
		 */
		public Builder<K, V> cacheHitTrackerL2(Consumer<Boolean> cacheHitTrackerL2) {
			this.cacheHitTrackerL2 = cacheHitTrackerL2;
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
		 * @return the CachingStateL2
		 */
		public CachingStateL2<K, V> build() {
			return new CachingStateL2<>(maxSize, maxSizeL2, state, operator, stateGetHook, stateUpdateHook, cacheHitTracker, cacheHitTrackerL2, readWatch, writeWatch);
		}
	}
}
