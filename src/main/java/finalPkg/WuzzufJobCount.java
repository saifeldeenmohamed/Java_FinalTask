/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalPkg;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

/**
 *
 * @author SaiF El-deen
 */
public class WuzzufJobCount {
    
    private static final String COMMA_DELIMITER = ",";
    
    public static String extractCompany(String jobLine) {
        try {
            return jobLine.split (COMMA_DELIMITER)[1];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    
    public static String extractTitle(String jobLine) {
        try {
            return jobLine.split (COMMA_DELIMITER)[0];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    
    public static String extractLocation(String jobLine) {
        try {
            return jobLine.split (COMMA_DELIMITER)[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    
    public static String extractSkills(String jobLine) {
        try {
            return jobLine.split (COMMA_DELIMITER)[7];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    
    
    public static void titlesFn(JavaRDD<String> jobs) {

    //-----------------------------------Exctact title ---------------------------
        JavaRDD<String> titles = jobs
                                     .map (WuzzufJobCount::extractTitle)
                                     .filter (StringUtils::isNotBlank);
        
       // JavaRDD<String>
//        JavaRDD<String> words = titles.flatMap (title -> Arrays.asList (title
//                                                                            .toLowerCase ()
//                                                                            .trim ()
//                                                                            .replaceAll ("\\p{Punct}", " ")
//                                                                            .split (" ")).iterator ());
//        System.out.println(titles.toString ());
        // COUNTING
        Map<String, Long> wordCounts = titles.countByValue ();
        List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
                                                       .sorted (Map.Entry.comparingByValue ())
                                                       .collect (Collectors.toList ());
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) 
        {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
    }
    
    public static void compainesFn(JavaRDD<String> jobs) {

    //-----------------------------------Exctact companies ---------------------------
        JavaRDD<String> companies = jobs
                                     .map (WuzzufJobCount::extractCompany)
                                     .filter (StringUtils::isNotBlank);
        
       // JavaRDD<String>
//        JavaRDD<String> words = companies.flatMap (title -> Arrays.asList (title
//                                                                            .toLowerCase ()
//                                                                            .trim ()
//                                                                            .replaceAll ("\\p{Punct}", " ")
//                                                                            .split (" ")).iterator ());
//        System.out.println(companies.toString ());
        // COUNTING
        Map<String, Long> wordCounts = companies.countByValue ();
        List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
                                                       .sorted (Map.Entry.comparingByValue ())
                                                       .collect (Collectors.toList ());
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) 
        {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
    }
    
    public static void locationFn(JavaRDD<String> jobs) {

    //-----------------------------------Exctact Areas ---------------------------
        JavaRDD<String> location = jobs
                                     .map (WuzzufJobCount::extractLocation)
                                     .filter (StringUtils::isNotBlank);
        
       // JavaRDD<String>
//        JavaRDD<String> words = companies.flatMap (title -> Arrays.asList (title
//                                                                            .toLowerCase ()
//                                                                            .trim ()
//                                                                            .replaceAll ("\\p{Punct}", " ")
//                                                                            .split (" ")).iterator ());
//        System.out.println(location.toString ());
        // COUNTING
        Map<String, Long> wordCounts = location.countByValue ();
        List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
                                                       .sorted (Map.Entry.comparingByValue ())
                                                       .collect (Collectors.toList ());
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) 
        {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
    }
    public static void skillsFn(JavaRDD<String> jobs) {

    //-----------------------------------Exctact Areas ---------------------------
        JavaRDD<String> skills = jobs
                                     .map (WuzzufJobCount::extractSkills)
                                     .filter (StringUtils::isNotBlank);
        
       // JavaRDD<String>
        JavaRDD<String> words = skills.flatMap (title -> Arrays.asList (title
                                                                            .toLowerCase ()
                                                                            .trim ()
                                                                            .replaceAll ("\\p{Punct}", " ")
                                                                            .split (" ")).iterator ());
        System.out.println(words.toString ());
        // COUNTING
        Map<String, Long> wordCounts = words.countByValue ();
        List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
                                                       .sorted (Map.Entry.comparingByValue ())
                                                       .collect (Collectors.toList ());
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) 
        {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
    }
}
