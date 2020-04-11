package com.bawei.iot;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.ParseException;
import java.text.SimpleDateFormat;

//完成补数功能
public class Complement {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String startTime = args[0];
        String endTime = args[1];


        DataSource<Tuple6<Long, Double, Double, Long, Long, Long>> fromhbase = env.createInput(new TableInputFormat<Tuple6<Long, Double, Double, Long, Long, Long>>() {
            @Override
            protected Scan getScanner() {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Scan scan = new Scan();
                try {
                    RowFilter filter1 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(sdf.parse(startTime).getTime())));
                    RowFilter filter2 = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes(sdf.parse(endTime).getTime())));
                    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);
                    scan.setFilter(filterList);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return scan;
            }

            @Override
            protected String getTableName() {
                return "t1:devstatics";
            }

            @Override
            protected Tuple6<Long, Double, Double, Long, Long, Long> mapResultToTuple(Result result) {
                long startTime = Bytes.toLong(result.getRow());
                double kaijilv = Bytes.toDouble(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("kaijilv")));
                double guzhanglv = Bytes.toDouble(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("guzhanglv")));
                long kaijishu = Bytes.toLong(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("kaijishu")));
                long guzhangshu = Bytes.toLong(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("guzhangshu")));
                long amount = Bytes.toLong(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("amount")));
                return Tuple6.of(startTime, kaijilv, guzhanglv, kaijishu, guzhangshu, amount);
            }
        });

        DataSource<String> text = env.readTextFile("hdfs://.....");
        MapOperator<String, Tuple6<String, String, String, String, String, Long>> fromhdfs = text.map(new MapFunction<String, Tuple6<String, String, String, String, String, Long>>() {
            @Override
            public Tuple6<String, String, String, String, String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple6.of(arr[0], arr[1], arr[2], arr[3], arr[4], Long.valueOf(arr[5]));
            }
        });

        GroupReduceOperator<Tuple6<String, String, String, String, String, Long>, Tuple3<String, Boolean, Long>> grouped = fromhdfs.groupBy(0, 5).reduceGroup(new GroupReduceFunction<Tuple6<String, String, String, String, String, Long>, Tuple3<String, Boolean, Long>>() {
            @Override
            public void reduce(Iterable<Tuple6<String, String, String, String, String, Long>> values, Collector<Tuple3<String, Boolean, Long>> out) throws Exception {

                boolean flag = false;
                String devId = "";
                Long timestamp = 0L;
                for (Tuple6<String, String, String, String, String, Long> value : values) {
                    devId = value.f0;
                    timestamp = value.f5;
                    if ("2".equals(value.f3)) {
                        flag = true;
                        break;
                    }
                }
                out.collect(Tuple3.of(devId, flag, timestamp));
            }
        });

    }
}
