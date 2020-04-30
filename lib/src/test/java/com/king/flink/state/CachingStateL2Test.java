package com.king.flink.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class CachingStateL2Test {
    public class MemState implements ValueState<String> {

        public Object currentKey = null;

        HashMap<Object, String> states = Mockito.spy(new HashMap<>());

        @Override
        public void clear() {
            states.remove(currentKey);
        }

        @Override
        public String value() throws IOException {
            return states.get(currentKey);
        }

        @Override
        public void update(String value) throws IOException {
            states.put(currentKey, value);
        }

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPushOut() throws IOException {
        MemState state = new MemState();
        for (int i = 1; i <= 5; i++) {
            state.states.put(i, "");
        }
        Mockito.clearInvocations(state.states);

        AbstractUdfStreamOperator<?, ?> op = Mockito.mock(AbstractUdfStreamOperator.class);
        when(op.getKeyedStateBackend()).thenReturn(Mockito.mock(KeyedStateBackend.class));
        Mockito.when(op.getCurrentKey()).then(i -> state.currentKey);
        Mockito.doAnswer(invocation -> {
            state.currentKey = invocation.getArgument(0);
            return null;
        }).when(op).setCurrentKey(Mockito.any());

        CachingStateL2<Object, String> cState = CachingStateL2.<Object, String>builder()
                .maxSize(5)
                .maxSizeL2(1)
                .state(state)
                .operator(op)
                .build();

        for (int i = 1; i <= 5; i++) {
            state.currentKey = i;
            cState.value();
        }

        Mockito.verify(state.states, Mockito.times(5)).get(Mockito.any());
        Mockito.clearInvocations(state.states);

        state.currentKey = 1;
        cState.update("v");

        for (int i = 2; i <= 5; i++) {
            state.currentKey = i;
            cState.value();
        }

        Mockito.verifyZeroInteractions(state.states);
        state.currentKey = 6;
        cState.update("6");
        Mockito.verify(state.states, Mockito.times(1)).put(Mockito.eq(1), Mockito.eq("v"));
    }

    /**
     * Tests a value evicted from L1 to L2 is updated to the underlying state.
     */
    @Test
    public void writeThroughTest() throws IOException {
        MemState state = new MemState();
        state.states.put(1, "a1");
        state.states.put(2, "b");

        AbstractUdfStreamOperator<?, ?> op = Mockito.mock(AbstractUdfStreamOperator.class);
        when(op.getKeyedStateBackend()).thenReturn(Mockito.mock(KeyedStateBackend.class));
        Mockito.when(op.getCurrentKey()).then(i -> state.currentKey);
        Mockito.doAnswer(invocation -> {
            state.currentKey = invocation.getArgument(0);
            return null;
        }).when(op).setCurrentKey(Mockito.any());

        CachingStateL2<Integer, String> cState = CachingStateL2.<Integer, String>builder()
                .maxSize(1)
                .maxSizeL2(1)
                .state(state)
                .operator(op)
                .build();

        Mockito.clearInvocations(state.states);

        state.currentKey = 1;
        assertEquals("a1", cState.value());
        cState.update("a2");
        assertEquals("a2", cState.value());
        Mockito.verify(state.states, Mockito.times(0)).put(Mockito.any(), Mockito.any());

        state.currentKey = 2;
        assertEquals("b", cState.value());

        Mockito.verify(state.states, Mockito.times(1)).put(1, "a2");
    }

    /**
     * Tests that hit trackers work as expected.
     */
    @Test
    public void cacheHitTrackerTest() throws IOException {
        MemState state = new MemState();
        state.states.put(1, "a");
        state.states.put(2, "b");

        AbstractUdfStreamOperator<?, ?> op = Mockito.mock(AbstractUdfStreamOperator.class);
        when(op.getKeyedStateBackend()).thenReturn(Mockito.mock(KeyedStateBackend.class));
        when(op.getCurrentKey()).then(i -> state.currentKey);
        Mockito.doAnswer(invocation -> {
            state.currentKey = invocation.getArgument(0);
            return null;
        }).when(op).setCurrentKey(Mockito.any());

        AtomicInteger l1Hits = new AtomicInteger();
        AtomicInteger l1Misses = new AtomicInteger();
        AtomicInteger l2Hits = new AtomicInteger();
        AtomicInteger l2Misses = new AtomicInteger();

        Consumer<Boolean> t1 = (hit) -> {
            if (hit) {
                l1Hits.incrementAndGet();
            } else {
                l1Misses.incrementAndGet();
            }
        };
        Consumer<Boolean> t2 = (hit) -> {
            if (hit) {
                l2Hits.incrementAndGet();
            } else {
                l2Misses.incrementAndGet();
            }
        };


        CachingStateL2<Integer, String> cState = CachingStateL2.<Integer, String>builder()
                .maxSize(1)
                .maxSizeL2(1)
                .state(state)
                .operator(op)
                .cacheHitTracker(t1)
                .cacheHitTrackerL2(t2)
                .build();

        // No hits....
        assertCacheHits(0, 0, 0, 0, l1Hits, l1Misses, l2Hits, l2Misses);

        // Full L1 & L2 miss
        state.currentKey = 1;
        assertEquals("a", cState.value());
        assertCacheHits(0, 1, 0, 1, l1Hits, l1Misses, l2Hits, l2Misses);

        // Found in L1
        assertEquals("a", cState.value());
        assertCacheHits(1, 1, 0, 1, l1Hits, l1Misses, l2Hits, l2Misses);

        // Full L1 & L2 miss
        state.currentKey = 2;
        assertEquals("b", cState.value());
        assertCacheHits(1, 2, 0, 2, l1Hits, l1Misses, l2Hits, l2Misses);

        // Found in L1
        assertEquals("b", cState.value());
        assertCacheHits(2, 2, 0, 2, l1Hits, l1Misses, l2Hits, l2Misses);

        // L1 miss, L2 hit
        state.currentKey = 1;
        assertEquals("a", cState.value());
        assertCacheHits(2, 3, 1, 2, l1Hits, l1Misses, l2Hits, l2Misses);

        // Found in L1
        assertEquals("a", cState.value());
        assertCacheHits(3, 3, 1, 2, l1Hits, l1Misses, l2Hits, l2Misses);
    }

    private void assertCacheHits(int l1h, int l1m, int l2h, int l2m, AtomicInteger l1Hits, AtomicInteger l1Misses, AtomicInteger l2Hits, AtomicInteger l2Misses) {
        assertEquals(l1h, l1Hits.get());
        assertEquals(l1m, l1Misses.get());
        assertEquals(l2h, l2Hits.get());
        assertEquals(l2m, l2Misses.get());
    }

    @Test
    public void mutatingGetHookTest() throws IOException {
        MemState state = new MemState();
        state.states.put(1, "a");
        state.states.put(2, "b");
        state.states.put(3, "c");

        AbstractUdfStreamOperator<?, ?> op = Mockito.mock(AbstractUdfStreamOperator.class);
        when(op.getKeyedStateBackend()).thenReturn(Mockito.mock(KeyedStateBackend.class));
        when(op.getCurrentKey()).then(i -> state.currentKey);
        Mockito.doAnswer(invocation -> {
            state.currentKey = invocation.getArgument(0);
            return null;
        }).when(op).setCurrentKey(Mockito.any());

        AtomicReference<StateHookResponse<String>> hookResponse = new AtomicReference<>();
        AtomicReference<Tuple2<Integer, String>> last = new AtomicReference<>();
        BiFunction<Integer, String, StateHookResponse<String>> hook = (a, b) -> {
            last.set(Tuple2.of(a, b));
            return hookResponse.get();
        };

        CachingStateL2<Integer, String> cState = CachingStateL2.<Integer, String>builder()
                .maxSize(1)
                .maxSizeL2(1)
                .state(state)
                .operator(op)
                .stateGetHook(hook)
                .build();

        assertNull(last.get());

        // Getting a value not the the cache issues causes a state get
        // and the hook gets called. The key 1 is now dirty in L1.
        state.currentKey = 1;
        hookResponse.set(new StateHookResponse.MutateStateHookResponse<>("a1"));
        assertEquals("a1", cState.value());
        assertEquals(Tuple2.of(1, "a"), last.get());
        last.set(null);

        // The same, but for another get
        // Now the 1st key gets written and the update hook called, because key 1 is dirty
        // (mutated by the get hook)
        state.currentKey = 2;
        hookResponse.set(new StateHookResponse.MutateStateHookResponse<>("b1"));
        assertEquals("b1", cState.value());
        assertEquals(Tuple2.of(2, "b"), last.get());
        last.set(null);

        hookResponse.set(StateHookResponse.IDENTITY);
        
        // Check they keys really stay modified
        state.currentKey = 1;
        assertEquals("a1", cState.value());
        state.currentKey = 2;
        assertEquals("b1", cState.value());
    }

    @Test
    public void mutatingUpdateHookTest() throws IOException {
        MemState state = new MemState();
        state.states.put(1, "a");
        state.states.put(2, "b");
        state.states.put(3, "c");

        AbstractUdfStreamOperator<?, ?> op = Mockito.mock(AbstractUdfStreamOperator.class);
        when(op.getKeyedStateBackend()).thenReturn(Mockito.mock(KeyedStateBackend.class));
        when(op.getCurrentKey()).then(i -> state.currentKey);
        Mockito.doAnswer(invocation -> {
            state.currentKey = invocation.getArgument(0);
            return null;
        }).when(op).setCurrentKey(Mockito.any());

        AtomicReference<StateHookResponse<String>> hookResponse = new AtomicReference<>();
        AtomicReference<Tuple2<Integer, String>> last = new AtomicReference<>();
        BiFunction<Integer, String, StateHookResponse<String>> hook = (a, b) -> {
            last.set(Tuple2.of(a, b));
            return hookResponse.get();
        };

        CachingStateL2<Integer, String> cState = CachingStateL2.<Integer, String>builder()
                .maxSize(1)
                .maxSizeL2(1)
                .state(state)
                .operator(op)
                .stateUpdateHook(hook)
                .build();

        assertNull(last.get());

        // Get a key, nothing happens
        state.currentKey = 1;
        assertEquals("a", cState.value());
        assertNull(last.get());

        // Make key 1 dirty
        cState.update("a1");

        // Get another key. Key 1 now in L2, update hook gets called because key 1 was dirty
        state.currentKey = 2;
        hookResponse.set(new StateHookResponse.MutateStateHookResponse<>("a2"));
        assertEquals("b", cState.value());
        assertEquals(Tuple2.of(1, "a1"), last.get());
        last.set(null);

        hookResponse.set(StateHookResponse.IDENTITY);

        // Check that key 1 really was changed
        state.currentKey = 1;
        assertEquals("a2", cState.value());
    }

    @Test
    public void sizeTest() throws IOException {
        List<Tuple2<Integer, String>> stateValues = Arrays.asList(new Tuple2[]
                        {
                                Tuple2.of(1, "a"),
                                Tuple2.of(2, "b"),
                                Tuple2.of(3, "c"),
                                Tuple2.of(4, "d"),
                                Tuple2.of(5, "e"),
                                Tuple2.of(6, "f"),
                                Tuple2.of(7, "g")
                        }
        );

        MemState state = new MemState();
        stateValues.stream()
                .forEach(t2 -> state.states.put(t2.f0, t2.f1));

        AbstractUdfStreamOperator<?, ?> op = Mockito.mock(AbstractUdfStreamOperator.class);
        when(op.getKeyedStateBackend()).thenReturn(Mockito.mock(KeyedStateBackend.class));
        Mockito.when(op.getCurrentKey()).then(i -> state.currentKey);
        Mockito.doAnswer(invocation -> {
            state.currentKey = invocation.getArgument(0);
            return null;
        }).when(op).setCurrentKey(Mockito.any());

        CachingStateL2<Integer, String> cs = CachingStateL2.<Integer, String>builder()
                .maxSize(3)
                .maxSizeL2(1)
                .state(state)
                .operator(op)
                .build();

        // Fits in l1
        assertSizes(stateValues, cs, state, null, asList(), asList());
        assertSizes(stateValues, cs, state, 1, asList(1), asList());
        assertSizes(stateValues, cs, state, 2, asList(1, 2), asList());
        assertSizes(stateValues, cs, state, 3, asList(1, 2, 3), asList());

        // No longer fits to L1, least recently used pushed to L2
        assertSizes(stateValues, cs, state, 4, asList(2, 3, 4), asList(1));

        // Requesting from L1 doesn't modify anything
        assertSizes(stateValues, cs, state, 2, asList(2, 3, 4), asList(1));
        assertSizes(stateValues, cs, state, 3, asList(2, 3, 4), asList(1));
        assertSizes(stateValues, cs, state, 4, asList(2, 3, 4), asList(1));

        // Requesting an entry that is in L2 promoted it to L1 and demotes L1 entry to L2
        assertSizes(stateValues, cs, state, 1, asList(1, 3, 4), asList(2));
        // Requesting from L1 doesn't modify anything now
        assertSizes(stateValues, cs, state, 1, asList(1, 3, 4), asList(2));
        assertSizes(stateValues, cs, state, 3, asList(1, 3, 4), asList(2));
        assertSizes(stateValues, cs, state, 4, asList(1, 3, 4), asList(2));

        // Add an entry that's not in the cache.
        // Pushed to L1, entry demoted from L1 to L2, L2 entry evicted
        assertSizes(stateValues, cs, state, 5, asList(3, 4, 5), asList(1));
        // Requesting from L1 doesn't modify anything now
        assertSizes(stateValues, cs, state, 3, asList(3, 4, 5), asList(1));
        assertSizes(stateValues, cs, state, 4, asList(3, 4, 5), asList(1));
        assertSizes(stateValues, cs, state, 5, asList(3, 4, 5), asList(1));
    }

    /**
     * If {@code key} is not null, makes gets the value of {@code key}
     * in the cache {@code cs} and asserts the value is correct, e.e.
     * matches the one in {@code stateValues}.
     * <p>
     * Runs the cycle for multiple times to better ensure idempotence.
     * </p>
     */
    private void assertSizes(List<Tuple2<Integer, String>> stateValues, CachingStateL2<Integer, String> cs, MemState state,
                             Integer key, List<Integer> l1, List<Integer> l2) throws IOException {
        for (int i = 0; i < 10; i++) {
            if (key != null) {
                Object previousKey = state.currentKey;
                state.currentKey = key;
                assertEquals(getValue(stateValues, key), cs.value());
                state.currentKey = previousKey;
            }

            assertEquals(sort(l1), sort(cs.getValues().keySet()));
            assertEquals(sort(l2), sort(cs.getValuesL2().keySet()));

            assertEquals(l1.size(), cs.sizeL1());
            assertEquals(l2.size(), cs.sizeL2());
            assertEquals(l1.size() + l2.size(), cs.size());
        }
    }

    private List<Integer> sort(Collection<Integer> c) {
        return c.stream().sorted().collect(Collectors.toList());
    }

    private String getValue(List<Tuple2<Integer, String>> stateValues, Integer key) {
        return stateValues.stream()
                .filter(t2 -> t2.f0.equals(key))
                .findFirst()
                .map(t2 -> t2.f1)
                .get();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cacheLogicTest() throws IOException {
        MemState state = new MemState();
        state.states.put(1, "a");
        state.states.put(2, "b");
        state.states.put(3, "c");
        Mockito.clearInvocations(state.states);

        AbstractUdfStreamOperator<?, ?> op = Mockito.mock(AbstractUdfStreamOperator.class);
        when(op.getKeyedStateBackend()).thenReturn(Mockito.mock(KeyedStateBackend.class));
        Mockito.when(op.getCurrentKey()).then(i -> state.currentKey);
        Mockito.doAnswer(invocation -> {
            state.currentKey = invocation.getArgument(0);
            return null;
        }).when(op).setCurrentKey(Mockito.any());

        CachingStateL2<Object, String> cState = CachingStateL2.<Object, String>builder()
                .maxSize(5)
                .maxSizeL2(1)
                .state(state)
                .operator(op)
                .build();

        state.currentKey = 1;
        assertEquals("a", cState.value());
        state.currentKey = 2;
        assertEquals("b", cState.value());
        state.currentKey = 3;
        assertEquals("c", cState.value());

        Mockito.verify(state.states, Mockito.times(3)).get(Mockito.any());
        Mockito.verify(state.states, Mockito.times(0)).put(Mockito.any(), Mockito.any());
        Mockito.verify(state.states, Mockito.times(0)).remove(Mockito.any());
        Mockito.clearInvocations(state.states);

        state.currentKey = 4;
        cState.update("d");
        state.currentKey = 5;
        cState.update("e");

        state.currentKey = 4;
        assertEquals("d", cState.value());
        state.currentKey = 5;
        assertEquals("e", cState.value());

        assertEquals(5, cState.size());

        Mockito.verifyZeroInteractions(state.states);

        state.currentKey = 1;
        cState.update("a2");
        state.currentKey = 2;
        cState.update("b2");

        assertEquals(5, cState.size());
        //assertTrue(cState.getValues().containsKey(3));

        state.currentKey = 6;
        cState.update("f");

        Mockito.verifyZeroInteractions(state.states);

        state.currentKey = 7;
        cState.update("g");

        Mockito.verify(state.states, Mockito.times(1)).put(Mockito.eq(4), Mockito.eq("d"));

        assertEquals(6, cState.size());
        //assertFalse(cState.getValues().containsKey(3));

        for (boolean update : Arrays.asList(true, false)) {
            assertEquals(5, cState.getValues().size());
            if (update) {
                cState.updateAllStates();
            }
            assertEquals(5, cState.getValues().size());

            state.currentKey = 1;
            assertEquals("a2", cState.value());
            state.currentKey = 2;
            assertEquals("b2", cState.value());
            state.currentKey = 3;
            assertEquals("c", cState.value());
            state.currentKey = 4;
            assertEquals("d", cState.value());
            state.currentKey = 5;
            assertEquals("e", cState.value());
            state.currentKey = 6;
            assertEquals("f", cState.value());
            state.currentKey = 7;
            assertEquals("g", cState.value());
        }

        for (int i = 1; i <= 7; i++) {
            state.currentKey = i;
            cState.clear();
        }

        for (int i = 1; i <= 7; i++) {
            state.currentKey = i;
            assertNull(cState.value());
        }
        assertTrue(state.states.isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFullFlush() throws IOException {
        MemState state = new MemState();
        Mockito.clearInvocations(state.states);

        AbstractUdfStreamOperator<?, ?> op = Mockito.mock(AbstractUdfStreamOperator.class);
        when(op.getKeyedStateBackend()).thenReturn(Mockito.mock(KeyedStateBackend.class));
        Mockito.when(op.getCurrentKey()).then(i -> state.currentKey);
        Mockito.doAnswer(invocation -> {
            state.currentKey = invocation.getArgument(0);
            return null;
        }).when(op).setCurrentKey(Mockito.any());

        CachingStateL2<Object, String> cState = CachingStateL2.<Object, String>builder()
                .maxSize(2)
                .maxSizeL2(1)
                .state(state)
                .operator(op)
                .build();

        state.currentKey = 1;
        cState.update("a");
        state.currentKey = 2;
        cState.update("b");

        Mockito.verifyZeroInteractions(state.states);

        state.currentKey = 3;
        cState.update("c");
        Mockito.verify(state.states, Mockito.times(1)).put(Mockito.eq(1), Mockito.eq("a"));

        assertEquals(3, cState.size());
    }
}

