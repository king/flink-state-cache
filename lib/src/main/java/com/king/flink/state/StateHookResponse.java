package com.king.flink.state;

public abstract class StateHookResponse<V> {
    public static final IdentityStateHookResponse IDENTITY = new IdentityStateHookResponse();

    /**
     * A response from a state get or state udpate hook indicating the value
     * was not changed.
     */
    public static class IdentityStateHookResponse<V> extends StateHookResponse<V> {
    }

    /**
     * A response from a state get or state udpate hook indicating the value
     * was changed.
     */
    public static class MutateStateHookResponse<V> extends StateHookResponse {
        private final V value;

        public MutateStateHookResponse(V value) {
            this.value = value;
        }

        public V getValue() {
            return value;
        }
    }
}
