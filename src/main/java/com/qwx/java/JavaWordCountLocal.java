package com.qwx.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaWordCountLocal {
    public static void main(String[] args) {

        if(args.length != 2){
            System.out.println("Usage:com.qwx.spark.WordCount <input><output>");
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName(JavaWordCountLocal.class.getSimpleName());
        //java api程序的入口
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //读取数据
        JavaRDD<String> lines = sc.textFile(args[0]);

        //切分并压平                         输入参数，输出参数
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //组装 单词 和 1                                                             输入   key        value
        JavaPairRDD<String, Integer> wordWithOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        //分组聚合 (a,b) => a+b                   a         b     返回值
        JavaPairRDD<String, Integer> result = wordWithOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        //排序                                                        上层输出作为本层输入                      输出key  value
        //1.k - v反转
        JavaPairRDD<Integer, String> reverseRes = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        //默认的规则是升序    降序
        JavaPairRDD<Integer, String> sortRes = reverseRes.sortByKey(false);

        //再反转                                                                       上层输出作为本层输入                      输出key  value
        JavaPairRDD<String, Integer> finalRes = sortRes.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });

        finalRes.saveAsTextFile(args[1]);

        sc.stop();
    }
}
