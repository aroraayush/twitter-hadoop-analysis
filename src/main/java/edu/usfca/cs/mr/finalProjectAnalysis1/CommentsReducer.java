package edu.usfca.cs.mr.finalProjectAnalysis1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

public class CommentsReducer extends Reducer<IntWritable, CommentsWritable, IntWritable, CommentsWritable> {

    private TreeMap<CommentsWritable, IntWritable> treeMap; // using treeMap to find the top 5 postId

    @Override
    protected void setup(Context context) {
        treeMap = new TreeMap<CommentsWritable, IntWritable>(new Comparator<CommentsWritable>() {
            @Override
            public int compare(CommentsWritable o1, CommentsWritable o2) {
                if(o1.getCommentsCount().get() > o2.getCommentsCount().get())
                    return -1;
                else if(o1.getCommentsCount().get() < o2.getCommentsCount().get())
                    return  1;
                else
                    return 1;
            }
        });
    }

    @Override
    protected void reduce(
            IntWritable key, Iterable<CommentsWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        double positive_sentiment = 0.0;
        double negative_sentiment = 0.0;
        int cp = 0;
        int cn = 0;
        CommentsWritable commentsWritable = new CommentsWritable();
        commentsWritable.setPostId(key.get());
        // adding relevant data to custom writable from the values
        for(CommentsWritable val : values){
            count++;
            commentsWritable.setCommentText(val.getCommentText());
            commentsWritable.setScore(val.getScore());
            commentsWritable.setUserId(val.getUserId());
            commentsWritable.setDate(val.getDate());
            if(val.getSentiment().get() >= 0.5) {
                positive_sentiment = positive_sentiment + val.getSentiment().get();
                cp++;
            }
            else {
                negative_sentiment = negative_sentiment + val.getSentiment().get();
                cn++;
            }
        }
        // finding the overall average sentiment
        double overall_sentiment = ((positive_sentiment / cp) + (negative_sentiment / cn)) / 2;
        commentsWritable.setCommentsCount(count);
        commentsWritable.setSentiment(overall_sentiment);
        treeMap.put(commentsWritable, key);
        // keeping top 5 values for top post Ids
        if(treeMap.size() > 5){
            treeMap.pollLastEntry();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // final output shows the top Ids and their associated data, along with sentiment value
        for(CommentsWritable val : treeMap.keySet()) {
//            System.out.println(val);
            context.write(val.getPostId(), val);
        }
    }
}
