package edu.usfca.cs.mr.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SentimentAnalysis {

    // Obtaning the overall sentiment w.r.t sets of tokens
//    public static double getSentiment(Set<String> hashSet, String positive_word_txt_path, String negative_word_txt_path) throws IOException {
//        int positive = 0;
//        int negative = 0;
//
//        BufferedReader br = new BufferedReader(
//                new FileReader(positive_word_txt_path));
//
//        BufferedReader br2 = new BufferedReader(
//                new FileReader(negative_word_txt_path));
//
//        String posWord = "";
//        String negWord = "";
//
//        while ((posWord = br.readLine()) != null){
//            if(hashSet.contains(posWord))
//                positive++;
//        }
//        while ((negWord = br2.readLine()) != null){
//            if(hashSet.contains(negWord))
//                negative++;
//        }
//        return ((double) (positive - negative)/ hashSet.size()) * 100;
//    }

    // Obtaning the overall sentiment w.r.t sets of tokens
    public static double getSentimentSet(Set<String> tokenSet, HashSet<String> positiveSet, HashSet<String> negativeSet) throws IOException {
        int positive = 0;
        int negative = 0;

        for(String token : tokenSet){
            if(positiveSet.contains(token)){
                positive++;
            }
            if(negativeSet.contains(token)){
                negative++;
            }
        }
        // Taking intersection of sets
//        tokenSet.retainAll(positiveSet);

        return ((double) (positive - negative)/ tokenSet.size()) * 100;
    }

    // Cleaning of the body, removing extra tokens that could increase load on calculating sentiment
    public static String cleanBody(String body) {
        String cleaned = body.replace("&lt;","")
                .replace("&gt","")
                .replace("pre;","")
                .replace("p;","")
                .replace(":","")
                .replace("I","")
                .replace("am","")
                .replace("."," ")
                .replace("on","")
                .replace("/","")
                .replace("&#xA;","")
                .replace("code;","");

        return cleaned;
    }


    // Cleaning of the title for post title (stack overflow dataset)
    public static String cleanTitle(String title) {
        String cleaned = title.replace("&quot;","");

        return cleaned;
    }
}
