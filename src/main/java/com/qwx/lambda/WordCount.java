package com.qwx.lambda;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext();

        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<String> words = lines.flatMap(t -> Arrays.asList(t.split(" ")).iterator());

        JavaPairRDD<Integer, Integer> wordWithOne = words.mapToPair(w -> new Tuple2<>(2, 1));

        JavaPairRDD<Integer, Integer> reduceData = wordWithOne.reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, Integer> swapData = reduceData.mapToPair(tp -> tp.swap());

        JavaPairRDD<Integer, Integer> sortData = swapData.sortByKey(false);

        JavaPairRDD<Integer, Integer> result = sortData.mapToPair(tp -> tp.swap());

        result.saveAsTextFile(args[1]);

        sc.stop();
    }
}
