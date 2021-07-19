/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalPkg;

import static finalPkg.WuzzufJobCount.compainesFn;
import static finalPkg.WuzzufJobCount.locationFn;
import static finalPkg.WuzzufJobCount.skillsFn;
import static finalPkg.WuzzufJobCount.titlesFn;
import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author SaiF El-deen
 */
public class MainClass {
    
    public static void main(String[] args) throws IOException{
        
        Logger.getLogger ("org").setLevel (Level.ERROR);
        // CREATE SPARK CONTEXT
        SparkConf conf = new SparkConf ().setAppName ("wordCounts").setMaster ("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext (conf);
        // LOAD DATASETS
        JavaRDD<String> jobs = sparkContext.textFile ("src/main/resources/Wuzzuf_Jobs.csv");
 
        // PRINTING
        System.out.println("titles =");
        titlesFn(jobs);
//        System.out.println("companies =");
//        compainesFn(jobs);
//        System2.out.println("Areas =");
//        locationFn(jobs);
//        System.out.println("Skills =");
//        skillsFn(jobs);
    }
}
