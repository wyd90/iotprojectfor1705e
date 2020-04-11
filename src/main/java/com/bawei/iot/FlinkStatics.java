package com.bawei.iot;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Properties;

public class FlinkStatics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node4:9092");
        properties.setProperty("group.id", "iotmessageconsumer");

        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>("iotmessage", new SimpleStringSchema(), properties);
        consumer011.setCommitOffsetsOnCheckpoints(true);
        consumer011.setStartFromGroupOffsets();

        DataStreamSource<String> message = env.addSource(consumer011);

        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, Long>> maped = message.map(new MapFunction<String, Tuple6<String, String, String, String, String, Long>>() {
            @Override
            public Tuple6<String, String, String, String, String, Long> map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String devId = jsonObject.getString("devId");
                String sId = jsonObject.getString("sId");
                String metric = jsonObject.getString("metric");
                String v = jsonObject.getString("v");
                String unit = jsonObject.getString("unit");
                Long t = Long.valueOf(jsonObject.getString("t"));
                return Tuple6.of(devId, sId, metric, v, unit, t);
            }
        });

        //计算开机工作的设备数量：三分钟之内有过alive报活信息的设备

        //抽取事件时间戳
        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, Long>> mapedWithEventTime = maped.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple6<String, String, String, String, String, Long>>() {

            private final long maxOutOfOrderness = 10000; // 10 seconds
            private long currentMaxTimestamp;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple6<String, String, String, String, String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f5;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
        });

        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, Long>> filtered = mapedWithEventTime.filter(new FilterFunction<Tuple6<String, String, String, String, String, Long>>() {
            @Override
            public boolean filter(Tuple6<String, String, String, String, String, Long> value) throws Exception {
                if ("alive".equals(value.f2)) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        final OutputTag<Tuple6<String, String, String, String, String, Long>> lateOutputTag = new OutputTag<Tuple6<String, String, String, String, String, Long>>("late-data"){};
        SingleOutputStreamOperator<Tuple4<String, Boolean, Long, Long>> applied = filtered.keyBy(0)
                .timeWindow(Time.minutes(3), Time.minutes(1))
                .sideOutputLateData(lateOutputTag)
                .apply(new WindowFunction<Tuple6<String, String, String, String, String, Long>, Tuple4<String, Boolean, Long, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple6<String, String, String, String, String, Long>> input, Collector<Tuple4<String, Boolean, Long, Long>> out) throws Exception {
                        //设备是否出现故障，false为无故障，true为有故障
                        boolean flag = false;
                        for (Tuple6<String, String, String, String, String, Long> value : input) {
                            if ("2".equals(value.f3)) {
                                flag = true;
                                break;
                            }
                        }
                        String devId = tuple.getField(0);
                        out.collect(Tuple4.of(devId, flag, window.getStart(),window.getEnd()));
                    }
                });
        //迟到数据流，存回kafka消息队列
        DataStream<Tuple6<String, String, String, String, String, Long>> outOfTimeData = applied.getSideOutput(lateOutputTag);

        SingleOutputStreamOperator<Tuple7<Double, Double, Long, Long, Long,Long,Long>> res = applied.keyBy(3).process(new KeyedProcessFunction<Tuple, Tuple4<String, Boolean, Long, Long>, Tuple7<Double, Double, Long, Long, Long, Long,Long>>() {

            private transient ValueState<Long> openNum;
            private transient ValueState<Long> failNum;
            private transient ValueState<Long> windowStart;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Long> openNumState = new ValueStateDescriptor<>(
                        "openNumState",
                        TypeInformation.of(Long.class),
                        0L
                );

                ValueStateDescriptor<Long> failNumState = new ValueStateDescriptor<>(
                        "failNum",
                        TypeInformation.of(Long.class),
                        0L
                );

                ValueStateDescriptor<Long> windowStartState = new ValueStateDescriptor<>(
                        "windowStart",
                        TypeInformation.of(Long.class),
                        0L
                );

                openNum = getRuntimeContext().getState(openNumState);
                failNum = getRuntimeContext().getState(failNumState);
                windowStart = getRuntimeContext().getState(windowStartState);
            }

            @Override
            public void processElement(Tuple4<String, Boolean, Long, Long> value, Context ctx, Collector<Tuple7<Double, Double, Long, Long, Long, Long,Long>> out) throws Exception {
                Long open = openNum.value();
                Long fail = failNum.value();
                if (value.f1) {
                    //有故障
                    open = open + 1;
                    fail = fail + 1;
                } else {
                    //无故障
                    open = open + 1;
                }
                openNum.update(open);
                failNum.update(fail);
                windowStart.update(value.f2);

                //注册定时器，当startTime所对应的窗口数据全部收集齐之后，开始运行定时器
                ctx.timerService().registerEventTimeTimer(value.f3 + 1L);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple7<Double, Double, Long, Long, Long, Long,Long>> out) throws Exception {
                Long kaijishu = openNum.value();
                Long guzhangshu = failNum.value();
                Long startTime = windowStart.value();

                openNum.clear();
                failNum.clear();
                windowStart.clear();

                double kaijilv = kaijishu / 2000L;
                double guzhanglv = guzhangshu / kaijishu;

                out.collect(Tuple7.of(kaijilv, guzhanglv, kaijishu, guzhangshu, 2000L, startTime,timestamp - 1L));
            }
        });

        res.print();

        env.execute();

        //res存hbase
        //hbase表设计
        // rowKey = startTime , f1:kaijilv  f1:guzhanglv f1:devamount  f1:kaijishu f1:guzhangshu f1:windowstart

        //方法一

//        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, Long>> dataFilterWithAlive = mapedWithEventTime.filter(new FilterFunction<Tuple6<String, String, String, String, String, Long>>() {
//            @Override
//            public boolean filter(Tuple6<String, String, String, String, String, Long> value) throws Exception {
//                if ("alive".equals(value.f2)) {
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        });
//
//        //报故障的设备信息
//        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, Long>> failDevData = dataFilterWithAlive.filter(new FilterFunction<Tuple6<String, String, String, String, String, Long>>() {
//            @Override
//            public boolean filter(Tuple6<String, String, String, String, String, Long> value) throws Exception {
//
//                if ("2".equals(value.f3)) {
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        });
//
//        //每分钟计算一次3分钟窗口时间内出现alive数据的设备数
//
//        final OutputTag<Tuple6<String, String, String, String, String, Long>> lateOutputTag = new OutputTag<Tuple6<String, String, String, String, String, Long>>("late-data"){};
//        SingleOutputStreamOperator<Tuple2<Long, Long>> windowed = dataFilterWithAlive
//                .keyBy(0)
//                .timeWindow(Time.minutes(3), Time.minutes(1))
//                .sideOutputLateData(lateOutputTag)
//                .apply(new WindowFunction<Tuple6<String, String, String, String, String, Long>, Tuple2<Long, Long>, Tuple, TimeWindow>() {
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple6<String, String, String, String, String, Long>> input, Collector<Tuple2<Long, Long>> out) throws Exception {
//                        out.collect(Tuple2.of(window.getStart(), 1L));
//                    }
//                });
//        //得到迟到数据，存入kafka队列中，等待补数
//        DataStream<Tuple6<String, String, String, String, String, Long>> sideOutputStream = windowed.getSideOutput(lateOutputTag);
//
//        //窗口开始时间和窗口内开机的数量
//        SingleOutputStreamOperator<Tuple2<Long, Long>> reduced = windowed
//                .keyBy(0)
//                .reduce(new ReduceFunction<Tuple2<Long, Long>>() {
//                    @Override
//                    public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
//                        //              窗口开始时间        开机数
//                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
//                    }
//                });
//
//        final OutputTag<Tuple6<String, String, String, String, String, Long>> faillateOutputTag = new OutputTag<Tuple6<String, String, String, String, String, Long>>("fail-late-data"){};
//        SingleOutputStreamOperator<Tuple2<Long, Long>> failWindowed = failDevData.keyBy(0)
//                .timeWindow(Time.minutes(3), Time.minutes(1))
//                .sideOutputLateData(faillateOutputTag)
//                .apply(new WindowFunction<Tuple6<String, String, String, String, String, Long>, Tuple2<Long, Long>, Tuple, TimeWindow>() {
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple6<String, String, String, String, String, Long>> input, Collector<Tuple2<Long, Long>> out) throws Exception {
//                        out.collect(Tuple2.of(window.getStart(), 1L));
//                    }
//                });
//
//        //3分钟内迟到的报故障的数据收集起来，发送到kafak，等待补数
//        DataStream<Tuple6<String, String, String, String, String, Long>> failsideOutput = failWindowed.getSideOutput(faillateOutputTag);
//
//        //窗口开始时间，和在窗口内报故障的设备数
//        SingleOutputStreamOperator<Tuple2<Long, Long>> failReduced = failWindowed
//                .keyBy(0)
//                .reduce(new ReduceFunction<Tuple2<Long, Long>>() {
//                    @Override
//                    public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
//                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
//                    }
//                });
//
//        SingleOutputStreamOperator<Tuple3<Long, Double, Double>> res = reduced.keyBy(0).intervalJoin(failReduced.keyBy(0))
//                .between(Time.seconds(-30), Time.seconds(30))
//                .process(new ProcessJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple3<Long, Double, Double>>() {
//                    @Override
//                    public void processElement(Tuple2<Long, Long> left, Tuple2<Long, Long> right, Context ctx, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
//                        double kaijilv = left.f1 / 2000L;
//                        double guzhanglv = right.f1 / left.f1;
//                        out.collect(Tuple3.of(left.f0, kaijilv, guzhanglv));
//                    }
//                });

        //方法二




        //把res存hbase（elasticsearch）

    }
}
