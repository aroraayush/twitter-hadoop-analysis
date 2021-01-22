package edu.usfca.cs.mr.stack_overflow_top5_posts;

import edu.usfca.cs.mr.util.SentimentAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.util.*;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class PostsMapper
extends Mapper<LongWritable, Text, IntWritable, WeeklyWritable> {

    private static Logger logger = LoggerFactory.getLogger(PostsMapper.class.getName());

    Date date;
    SimpleDateFormat df;
    ZonedDateTime zDt;
    HashSet<String> positiveWords;
    HashSet<String> negativeWords;

    
    @Override
    protected void setup(Context context) throws IOException {
        df = new SimpleDateFormat("yyyy-MM-dd");
        positiveWords = new HashSet<>();
        negativeWords = new HashSet<>();

        Configuration conf = context.getConfiguration();

        String positive_word_txt_path = conf.get("positive_word_txt_path");
        String negative_word_txt_path = conf.get("negative_word_txt_path");

        Path pt= new Path(positive_word_txt_path);//Location of file in HDFS

        FileSystem fs = FileSystem.get(conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
        while (line != null){
            positiveWords.add(line.trim());
            line=br.readLine();
        }


        Path pt2= new Path(negative_word_txt_path);//Location of file in HDFS

        BufferedReader br2=new BufferedReader(new InputStreamReader(fs.open(pt2)));
        String line2;
        line2=br2.readLine();
        while (line2 != null){
            negativeWords.add(line2.trim());
            line2=br2.readLine();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        try {
            String line = value.toString();
            // Check if line is invalid
            if(line == null || line.isEmpty() || line.trim().length() == 0)
                return;
            line = line.trim();

            // Checking if valid line as per our requirement
            if(line.startsWith("<row")){

                // Extracting the body
                String body = line.split("Body=")[1].split("\"",3)[1];

                // Extracting the title
                String title = line.split("Title=")[1].split("\"",3)[1];

                // Removing extra quotes (") from string
                line = line.replace("\"", "");

                String[] attributes = line.split(" ");
                int score = 0;
                int answerCount = 0;
                int week = -1;
                Text dateStr = null;

                // Extracting the meta data
                for(String attribute : attributes){
                    String[] attr = attribute.split("=");
                    if(attr.length > 1){
                        String attrKey = attr[0].trim();
                        String attrVal = attr[1].trim();
                        switch (attrKey){
                            case "Score":
                                score = Integer.parseInt(attrVal);
                                break;
                            case "LastEditDate":
                                dateStr = new Text(attrVal.substring(0,16).replace("T"," "));
                                date = df.parse(attrVal);
                                zDt = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
                                week = zDt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
                                break;
                            case "AnswerCount":
                                answerCount = Integer.parseInt(attrVal);
                                break;
                            default:
                                break;
                        }
                    }

                }

                // Checking overall validity of data
                if(week!=-1) {
                    // cleaning the body
                    String cleaned = SentimentAnalysis.cleanBody(body);
                    // obtained the token from cleaned body for sentiment analysis
                    Set<String> tokens = getTokens(cleaned);
                    // obtaining the overall sentiment
                    double sentiment = SentimentAnalysis.getSentimentSet(tokens, positiveWords ,negativeWords);

                    //  Creating a list of posts, which will be added to a week number below
                    List<PostWritable> postList = new ArrayList<>();

                    // Creating instance for writable for a post, containing metadata title, data of post, score, answer count
                    PostWritable pW = new PostWritable(sentiment, new Text(SentimentAnalysis.cleanTitle(title)),dateStr, score,answerCount);
                    postList.add(pW);

                    // Storing data for a week along with all the posts for the week
                    WeeklyWritable wW = new WeeklyWritable(1, week, postList);
                    context.write(new IntWritable(week), wW);
                }
            }

        }
        catch (Exception e){
            logger.error("PostsMapper" + e.getMessage());
        }

    }

    private Set<String> getTokens(String body) {
        // Storing the string tokens for sentiment analysis
        Set<String> tokens = new HashSet<>();
        if(body!=null){
            StringTokenizer tokenizer = new StringTokenizer(body);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().trim();
                // Checking if the token is invalid
                if (!token.isEmpty()) {
                    Text t = new Text(token);
                    tokens.add(token);
                }
            }
        }
        return tokens;
    }
}
