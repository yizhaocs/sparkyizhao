import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args){
        // reduceExample();
        mapExample();
    }

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

        sqrtRdd.foreach(System.out::println);
        System.out.println("result:" + result); // result:158.63943

        sc.close();
    }
}
