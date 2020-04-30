package com.king.flink.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static com.king.flink.state.Example.Event.EVENT_TYPE_STATE_FLUSH;
import static com.king.flink.state.Example.Event.EVENT_TYPE_STATE_UPDATE;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;

/**
 * Demonstrates using {@link CachingStateL2}.
 */
public class Example {
    public static final int EVENTS_IN_BATCH = 1000;
    public static final int WAIT_AFTER_EVENT_BATCH = 1;
    private static final boolean USE_MANAGED_MEMORY = false;
    private static final int PARALLELISM = 4;
    /**
     * The count of unique users in the generated events.
     */
    public static long UID_COUNT = 5_000;

    /**
     * The count of unique state keys.
     */
    public static int KEY_COUNT = 100;

    public static int CACHE_L1_SIZE = 100;
    public static int CACHE_L2_SIZE = 500;

    private final String[] args;

    public Example(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) throws Exception {
        new Example(args).run();
    }

    private void run() throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        Configuration configuration = new Configuration();
        if (!USE_MANAGED_MEMORY) {
            configuration.setBoolean(RocksDBOptions.USE_MANAGED_MEMORY, USE_MANAGED_MEMORY);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM, configuration);
        env.enableCheckpointing(1000L);



        Path tempDirPath = Files.createTempDirectory("example");
        StateBackend backend = new RocksDBStateBackend("file://" + tempDirPath.toString(), true);
        env.setStateBackend(backend);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Event> events = env.addSource(new ExampleSource());
        KeyedStream<Event, Long> keyedEvents = events.keyBy(Event::getUid);

        SingleOutputStreamOperator<Double> avg = keyedEvents.transform("magic", DOUBLE_TYPE_INFO,
                new ExampleOneInputStreamOperator());
        KeyedStream<Double, String> doubleStringKeyedStream = avg.keyBy((d) -> "dummy");
        WindowedStream<Double, String, TimeWindow> window = doubleStringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        SingleOutputStreamOperator<Double> max = window.max(0);
        max.print();

        env.execute("Example");
    }

    public static class ExampleOneInputStreamOperator
            extends AbstractStreamOperator<Double>
            implements OneInputStreamOperator<Event, Double> {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        private transient CachingStateL2<Long, Map<String, Long>> cachedState;

        private transient long l1Hits = 0L;
        private transient long l2Hits = 0L;
        private transient long misses = 0L;

        @Override
        public void open() throws Exception {
            ValueState<Map<String, Long>> state = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("state", new MapTypeInfo<>(String.class, Long.class)));

            cachedState = CachingStateL2.<Long, Map<String, Long>>builder()
                    .maxSize(CACHE_L1_SIZE)
                    .maxSizeL2(CACHE_L2_SIZE)
                    .operator(this)
                    .state(state)
                    .cacheHitTracker(l1Hit -> l1Hits++)
                    .cacheHitTrackerL2(l2Hit -> { if (l2Hit) { l2Hits++; } else { misses++; } })
                    .build();

            super.open();
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            logger.info("Snapshotting: l1 hits {} l2 hits {} misses {} subtask {} checkpoint id {} ", l1Hits, l2Hits, misses, getRuntimeContext().getIndexOfThisSubtask(), context.getCheckpointId());
            cachedState.updateAllStates();
            super.snapshotState(context);
        }

        @Override
        public void processElement(StreamRecord<Event> element) throws Exception {
            Map<String, Long> map = cachedState.value();

            Event event = element.getValue();
            Tuple2<String, Long> payload = event.getPayload();

            if (map == null) {
                map = new HashMap<>();
                cachedState.update(map);
            }
            switch (event.getType()) {
                case EVENT_TYPE_STATE_UPDATE:
                    String key = payload.f0;
                    Long value = payload.f1;
                    map.merge(key, value, (oldValue, newValue) -> oldValue + newValue);

                    cachedState.update(map);
                    return;
                case EVENT_TYPE_STATE_FLUSH:
                    double avg = (double) map.values().stream()
                            .collect(Collectors.summingLong(Long::longValue))
                            /
                            (double) map.size();

                    this.output.collect(element.replace(avg));
                    return;
                default:
                    throw new IllegalArgumentException("" + event);
            }
        }
    }

    public static class Event {
        public static final int EVENT_TYPE_STATE_UPDATE = 0;
        public static final int EVENT_TYPE_STATE_FLUSH = 1;

        private int type;
        private long uid;
        private Tuple2<String, Long> payload;

        public Event() {
        }

        public Event(int type, long uid, Tuple2<String, Long> payload) {
            this.type = type;
            this.uid = uid;
            this.payload = payload;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public long getUid() {
            return uid;
        }

        public void setUid(long uid) {
            this.uid = uid;
        }

        public Tuple2<String, Long> getPayload() {
            return payload;
        }

        public void setPayload(Tuple2<String, Long> payload) {
            this.payload = payload;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "type=" + type +
                    ", uid=" + uid +
                    ", payload=" + payload +
                    '}';
        }
    }

    /**
     * A {@link SourceFunction} that creates a sequence of {@link Event}s.
     * <p>
     * The sequence starts with {@link #EVENTS_IN_BATCH} events of type {@link Event#EVENT_TYPE_STATE_UPDATE}.
     * Each event has a random uid from a pool of {@link #UID_COUNT} unique
     * user ids and a unique key from a pool of {@link #KEY_COUNT} unique keys.
     * <p>
     * Then the source sends a "flush" event ({@link Event#EVENT_TYPE_STATE_FLUSH}) for each user id.
     * <p>
     * Then the source starts from the beginning.
     */
    private static class ExampleSource implements SourceFunction<Event> {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random(123);

            long eventCount = 0;
            while (true) {
                for (int i = 0; i < EVENTS_IN_BATCH; i++) {
                    int key = random.nextInt(KEY_COUNT) + 1;
                    long uid = Math.abs(random.nextLong() % UID_COUNT) + 1;
                    ctx.collect(new Event(0, uid, Tuple2.of("key_" + key, Math.abs(random.nextLong() % 1_000))));
                    eventCount++;
                }
                if (!running) {
                    break;
                }

                Thread.sleep(WAIT_AFTER_EVENT_BATCH);

                for (long uid = 1; uid <= UID_COUNT; uid++) {
                    ctx.collect(new Event(1, uid, null));
                }

                if (eventCount % 100_000 == 0) {
                    logger.debug("eventCount {}", eventCount);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

