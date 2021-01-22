//package edu.usfca.cs.mr.sentiment_analysis;
//
//import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//
//import java.util.Properties;
//
//public class Pipeline {
//
//    private static Properties props;
////    private static StanfordCoreNLP standfordCoreNLP;
//
//    public Pipeline() { }
//
//    static {
//        props = new Properties();
//        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
//    }
//
//    public static StanfordCoreNLP getPipeline() {
//        if(standfordCoreNLP == null){
//            standfordCoreNLP = new StanfordCoreNLP(props);
//        }
//        return standfordCoreNLP;
//    }
//}
