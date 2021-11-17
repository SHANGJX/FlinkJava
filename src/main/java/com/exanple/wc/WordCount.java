package com.exanple.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        //1 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2 从文件中读取数据
        String inputPath = "G:\\Project\\FlinkJava\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataset = env.readTextFile(inputPath);

        //3 空格分词打散后， 对单词进行group by 分组， 然后用sum 聚合
        DataSet<Tuple2<String, Integer>> wordCountDataset =
                inputDataset.flatMap(new MyFlatMapper())
                        .groupBy(0)
                        .sum(1);

        wordCountDataset.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}

