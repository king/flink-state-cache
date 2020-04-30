package com.king.flink.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class CachingStateTest {
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
		when(op.getCurrentKey()).then(i -> state.currentKey);
		Mockito.doAnswer(invocation -> {
			state.currentKey = invocation.getArgument(0);
			return null;
		}).when(op).setCurrentKey(Mockito.any());

		CachingState<Object, String> cState = CachingState.<Object, String>builder()
				.maxSize(5)
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
		when(op.getCurrentKey()).then(i -> state.currentKey);
		Mockito.doAnswer(invocation -> {
			state.currentKey = invocation.getArgument(0);
			return null;
		}).when(op).setCurrentKey(Mockito.any());

		CachingState<Object, String> cState = CachingState.<Object, String>builder()
				.maxSize(5)
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

		assertEquals(5, cState.getValues().size());

		Mockito.verifyZeroInteractions(state.states);

		state.currentKey = 1;
		cState.update("a2");
		state.currentKey = 2;
		cState.update("b2");

		assertEquals(5, cState.getValues().size());
		assertTrue(cState.getValues().containsKey(3));

		state.currentKey = 6;
		cState.update("f");

		Mockito.verifyZeroInteractions(state.states);

		state.currentKey = 7;
		cState.update("g");

		Mockito.verify(state.states, Mockito.times(1))
				.put(Mockito.eq(4), Mockito.eq("d"));

		assertEquals(5, cState.getValues().size());
		assertFalse(cState.getValues().containsKey(3));

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
		when(op.getCurrentKey()).then(i -> state.currentKey);
		Mockito.doAnswer(invocation -> {
			state.currentKey = invocation.getArgument(0);
			return null;
		}).when(op).setCurrentKey(Mockito.any());

		CachingState<Object, String> cState = CachingState.<Object, String>builder()
				.maxSize(2)
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

		assertEquals(2, cState.getValues().size());
	}


}
