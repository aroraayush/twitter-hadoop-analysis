//package edu.usfca.cs.mr.sentiment_analysis;
//
//import edu.stanford.nlp.pipeline.CoreDocument;
//import edu.stanford.nlp.pipeline.CoreSentence;
//import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//import java.text.SimpleDateFormat;
//import java.time.ZoneId;
//import java.time.ZonedDateTime;
//import java.time.temporal.IsoFields;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.StringTokenizer;
//
///**
// * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
// */
//public class HashTagSAMapper
//extends Mapper<IntWritable, Text, IntWritable, WeekSAWritable> {
//
//    String twitterStr = "U\thttp://twitter.com/";
//    int len = twitterStr.length();
//    StanfordCoreNLP pipeline;
//
//    @Override
//    protected void setup(Context context) {
//        pipeline = Pipeline.getPipeline();
//    }
//
//    @Override
//    protected void map(IntWritable key, Text value, Context context) {
//
//        try {
//            String line = value.toString();
//            if(line == null || line.isEmpty() || line.trim().length() == 0)
//                return;
//            if(line.contains("#")){
//
//                Date date;
//                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//                ZonedDateTime zDt = null;
//                String dateTimestamp;
//
//                String[] lines = line.split("\n");
//                List<HashtagSAWritable> hashTagList = new ArrayList<>();
//
//                int week = -1;
//
//                for(String l : lines){
//                    if(l.startsWith("T")){
//                        dateTimestamp = l.substring(2,18);
//                        date = df.parse(dateTimestamp);
//                        zDt = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
//                        week = zDt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
//                    }
//                    else if(l.startsWith("W")) {
//                        StringTokenizer tokenizer;
//
//                        CoreDocument document = new CoreDocument(l);
//                        // annnotate the document
//                        pipeline.annotate(document);
//                        List<CoreSentence> sentences = document.sentences();
//
//                        for(CoreSentence sentence: sentences){
//                            String sentiment = sentence.sentiment();
//                            tokenizer = new StringTokenizer(sentence.toString());
//
//                            while (tokenizer.hasMoreTokens()) {
//                                String token = tokenizer.nextToken();
//                                if (token.charAt(0) == '#') {
//                                    HashtagSAWritable hW = new HashtagSAWritable(1,new Text(token),new Text(sentiment));
//                                    hashTagList.add(hW);
//                                }
//                            }
//                        }
//                    }
//                }
//                if(week!=-1 && hashTagList.size() != 0) {
//                    WeekSAWritable wW = new WeekSAWritable(1, week, hashTagList);
//                    context.write(new IntWritable(week), wW);
//                }
//            }
//        }
//        catch (Exception e){
//            e.printStackTrace();
//        }
//
//    }
//
//}
