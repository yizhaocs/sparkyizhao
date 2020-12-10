import com.google.inject.internal.cglib.proxy.$FixedValue;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple22;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args){
        // loadCollectionIntoRDDExample();
        // reduceExample();
        mapExample();
        // printEachItemInRDDExample();
        // countInRDDExample();
        // tuplesExample();
        // pairRDDExample();
        // countInPairRDD();
    }


    /*
        using JavaSparkContext's parallelize method to load collection into RDD
     */
    public static void loadCollectionIntoRDDExample(){
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        /*
            local uses 1 thread only.
            local[n] uses n threads.
            local[*] uses as many threads as your spark local machine have, where you are running your application.
         */
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        sc.close();
    }

    /*
        reduce means reduce the collection into the single answer
     */
    public static void reduceExample(){
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        /*
            local uses 1 thread only.
            local[n] uses n threads.
            local[*] uses as many threads as your spark local machine have, where you are running your application.
         */
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        {
            /////////////////////////////////////////以下是Production Code/////////////////////////////////////////////////////////////////
            Double result = sc.parallelize(inputData).reduce((value1, value2) -> value1 + value2);
            System.out.println("result:" + result); // result:158.63943
        }

        {
            /////////////////////////////////////////以下是Easy Debug Code/////////////////////////////////////////////////////////////////
            JavaRDD<Double> myRdd = sc.parallelize(inputData);

            Double result = myRdd.reduce((value1, value2) -> value1 + value2);
            System.out.println("result:" + result); // result:158.63943
        }
        sc.close();
    }

    /*
        The map method on an RDD allows us to transform the structure of RRD from one to another.
     */
    public static void mapExample(){
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);



        /*
            local uses 1 thread only.
            local[n] uses n threads.
            local[*] uses as many threads as your spark local machine have, where you are running your application.
         */
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        {
            /////////////////////////////////////////以下是Production Code/////////////////////////////////////////////////////////////////
            /*
                5.916079783099616
                3.4641016151377544
                9.486832980505138
                4.47213595499958
             */
            sc.parallelize(inputData).map(value -> Math.sqrt(value)).collect().forEach(System.out::println);
        }
        {
            /////////////////////////////////////////以下是Easy Debug Code/////////////////////////////////////////////////////////////////
            JavaRDD<Integer> myRdd = sc.parallelize(inputData);

            Integer result = myRdd.reduce((value1, value2) -> value1 + value2);

            JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));
            /*
                5.916079783099616
                3.4641016151377544
                9.486832980505138
                4.47213595499958
             */
            sqrtRdd.collect().forEach(System.out::println);
            System.out.println("result:" + result); // result:157
        }
        sc.close();
    }

    /*
        print Each Item In RDD
    */
    public static void printEachItemInRDDExample(){
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        /*
            local uses 1 thread only.
            local[n] uses n threads.
            local[*] uses as many threads as your spark local machine have, where you are running your application.
         */
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        {
            /////////////////////////////////////////以下是Production Code/////////////////////////////////////////////////////////////////
            /*
                35
                12
                90
                20
             */
            sc.parallelize(inputData).collect().forEach(System.out::println);
        }
        {
            /////////////////////////////////////////以下是Easy Debug Code/////////////////////////////////////////////////////////////////
            JavaRDD<Integer> myRdd = sc.parallelize(inputData);

            /*
                35
                12
                90
                20
             */
            myRdd.collect().forEach(System.out::println);
        }
        sc.close();
    }

    /*
        count How Many Items In RDD
     */
    public static void countInRDDExample(){
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        /*
            local uses 1 thread only.
            local[n] uses n threads.
            local[*] uses as many threads as your spark local machine have, where you are running your application.
         */
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        {
            /////////////////////////////////////////以下是Production Code/////////////////////////////////////////////////////////////////
            Integer count = sc.parallelize(inputData).map(value -> 1).reduce((value1, value2) -> value1 + value2);
            System.out.println("count:" + count); // 4
        }
        {
            /////////////////////////////////////////以下是Easy Debug Code/////////////////////////////////////////////////////////////////
            JavaRDD<Integer> myRdd = sc.parallelize(inputData);

            /*
                We use the map process but we just simply map every single value in RDD to the output value of 1 and then we do reduce,
                and reduce can simply add up all of the values in the second RDD.
             */
            JavaRDD<Integer> singleIntegerRdd = myRdd.map(value -> 1);
            Integer count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
            System.out.println("count:" + count); // 4
        }
        sc.close();
    }

    /*
        Tuples的话，存多达22个fields的collection
     */
    public static void tuplesExample(){
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        /*
            local uses 1 thread only.
            local[n] uses n threads.
            local[*] uses as many threads as your spark local machine have, where you are running your application.
         */
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        JavaRDD<Tuple2<Integer, Double>> twoElementTuples = myRdd.map(value -> new Tuple2<>(value, Math.sqrt(value)));
        /*
            (35,5.916079783099616)
            (12,3.4641016151377544)
            (90,9.486832980505138)
            (20,4.47213595499958)
         */
        twoElementTuples.collect().forEach(System.out::println);

        JavaRDD<Tuple4<Integer, Double,Integer,Integer>> fourElementTuples = myRdd.map(value -> new Tuple4<>(value, Math.sqrt(value), value + 1, value + 10));
        /*
            (35,5.916079783099616,36,45)
            (12,3.4641016151377544,13,22)
            (90,9.486832980505138,91,100)
            (20,4.47213595499958,21,30)
         */
        fourElementTuples.collect().forEach(System.out::println);

        // 甚至22个elements的collection也可以用scala lib来新建
        /*
            (35,5.916079783099616,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55)
            (12,3.4641016151377544,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32)
            (90,9.486832980505138,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110)
            (20,4.47213595499958,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40)
         */
        JavaRDD<Tuple22<Integer,Double,Integer,Integer,Integer
                ,Integer,Integer,Integer,Integer,Integer
                ,Integer,Integer,Integer,Integer,Integer
                ,Integer,Integer,Integer,Integer,Integer
                ,Integer,Integer>> twentyTwoElementTuples = myRdd.map(value -> new Tuple22<>(value, Math.sqrt(value),
                value + 1, value + 2, value + 3, value + 4, value + 5, value + 6, value + 7, value + 8
                , value + 9, value + 10, value + 11, value + 12, value + 13, value + 14, value + 15, value + 16, value + 17
                , value + 18, value + 19, value + 20));
        twentyTwoElementTuples.collect().forEach(System.out::println);
    }

    /*
         PairRDD的话，存2个fields的collection
     */
    public static void pairRDDExample(){
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");



        /*
            local uses 1 thread only.
            local[n] uses n threads.
            local[*] uses as many threads as your spark local machine have, where you are running your application.
         */
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        {
            /////////////////////////////////////////以下是Production Code//////////////////////////////////////////////////////////////////
            /*
                (WARN, Tuesday 4 September 0405)
                (ERROR, Tuesday 4 September 0408)
                (FATAL, Wednesday 5 September 1632)
                (ERROR, Friday 7 September 1854)
                (WARN, Saturday 8 September 1942)
             */
            sc.parallelize(inputData)
                    .mapToPair(value -> new Tuple2<>(value.split(":")[0], value.split(":")[1]))
                    .collect().forEach(System.out::println);
        }
        {
            /////////////////////////////////////////以下是Easy Debug Code/////////////////////////////////////////////////////////////////
            JavaRDD<String> myRdd = sc.parallelize(inputData);

            JavaPairRDD<String, String> pairRdd = myRdd.mapToPair(value -> {
                String[] columns = value.split("\\:");
                String level = columns[0];
                String date = columns[1];
                return new Tuple2<>(level, date);
            });

            /*
                (WARN, Tuesday 4 September 0405)
                (ERROR, Tuesday 4 September 0408)
                (FATAL, Wednesday 5 September 1632)
                (ERROR, Friday 7 September 1854)
                (WARN, Saturday 8 September 1942)
             */
            pairRdd.collect().forEach(System.out::println);
        }
        sc.close();
    }

    /*
        count Values With Same Key In PairRDD
     */
    public static void countInPairRDD(){
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");



        /*
            local uses 1 thread only.
            local[n] uses n threads.
            local[*] uses as many threads as your spark local machine have, where you are running your application.
         */
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        {
            /////////////////////////////////////////以下是Production Code//////////////////////////////////////////////////////////////////
             /*
                FATAL has 1 instances
                WARN has 2 instances
                ERROR has 2 instances
             */
            sc.parallelize(inputData)
                    .mapToPair(value -> new Tuple2<>(value.split(":")[0], 1L))
                    .reduceByKey((value1, value2) -> value1 + value2)
                    .collect().forEach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
        }
        {
            /////////////////////////////////////////以下是Easy Debug Code/////////////////////////////////////////////////////////////////
            JavaRDD<String> myRdd = sc.parallelize(inputData);

            JavaPairRDD<String, Long> pairRdd = myRdd.mapToPair(value -> {
                String[] columns = value.split("\\:");
                String level = columns[0];
                String date = columns[1];
                return new Tuple2<>(level, 1L);
            });

            JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
        /*
            FATAL has 1 instances
            WARN has 2 instances
            ERROR has 2 instances
         */
            sumsRdd.collect().forEach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
        }
        sc.close();
    }
}
