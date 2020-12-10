import com.google.inject.internal.cglib.proxy.$FixedValue;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args){
        // loadCollectionIntoRDDExample();
        // reduceExample();
        // mapExample();
        printEachItemInRDDExample();
        // countHowManyItemsInRDDExample();
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

        JavaRDD<Double> myRdd = sc.parallelize(inputData);

        Double result = myRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println("result:" + result); // result:158.63943

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

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);

        JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));

        sqrtRdd.collect().forEach(System.out::println);
        System.out.println("result:" + result); // result:157

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

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        /*
            35
            12
            90
            20
         */
        myRdd.collect().forEach(System.out::println);

        sc.close();
    }

    /*
        count How Many Items In RDD
     */
    public static void countHowManyItemsInRDDExample(){
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

        /*
            We use the map process but we just simply map every single value in RDD to the output value of 1 and then we do reduce,
            and reduce can simply add up all of the values in the second RDD.
         */
        JavaRDD<Integer> singleIntegerRdd = myRdd.map(value -> 1);
        Integer count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println("count:" + count); // 4

        sc.close();
    }
}
