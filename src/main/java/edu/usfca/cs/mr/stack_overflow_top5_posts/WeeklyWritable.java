package edu.usfca.cs.mr.stack_overflow_top5_posts;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * It contains the week number, total post count in the week
 *  and all the posts data for the week.
 */
public class WeeklyWritable implements Writable {

    private IntWritable week;   // week number
    private IntWritable count;  // total post count for the week
    private List<PostWritable> postList;    // posts for the week

    //default constructor for (de)serialization
    public WeeklyWritable() {
        this.count = new IntWritable(0);
        this.week = new IntWritable(0);
        this.postList = new ArrayList<>();
    }

    public WeeklyWritable(int count, int week, List<PostWritable> postList) {
        this.count = new IntWritable(count);
        this.week = new IntWritable(week);
        this.postList = new ArrayList<>();
        this.postList.addAll(postList);
    }

    public void write(DataOutput out) throws IOException {
        // Serializing the data to send to next machine
        this.count.write(out);
        this.week.write(out);
        out.writeInt(postList.size());

        for(int index=0;index<postList.size();index++){
            // Serializing every values in list to send to next machine
            postList.get(index).write(out); //write all the value of list
        }
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.count.readFields(in);
        this.week.readFields(in);

        int size = in.readInt(); //read size of list
        postList = new ArrayList<>(size);

        for(int i=0;i<size;i++){ //read all the values of list
            PostWritable hw = new PostWritable();
            hw.readFields(in);
            postList.add(hw);
        }
    }

    public IntWritable getWeek() {
        return week;
    }

    public void setWeek(IntWritable week) {
        this.week = week;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = new IntWritable(this.count.get()+count.get());
    }

    public List<PostWritable> getPostList() {
        return postList;
    }

    public void setPostList(List<PostWritable> postList) {
        this.postList = postList;
    }

    public void addAllPostList(List<PostWritable> postList) {
        this.postList.addAll(postList);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\t\t Total posts " + count);
        sb.append(System.lineSeparator());
        for(int i = 0 ; i< postList.size();i++){
            sb.append(System.lineSeparator());
            sb.append("\t\t\t" + postList.get(i).getDate());
            sb.append("\tTitle: " + postList.get(i).getTitle());
            sb.append(System.lineSeparator());
            sb.append("\t\t\t\t\t\t\t\tSentiment: ");
            double sentiment1 = postList.get(i).getSentiment().get();
            if(sentiment1 > 0){
                sb.append(sentiment1 + "% positive");
            }
            else if(sentiment1 == 0){
                sb.append("neutral");
            }
            else {
                sb.append((-1 * sentiment1) + "% negative");
            }
            sb.append("\tScore: " + postList.get(i).getScore());
            sb.append("\tAnswer Count: " + postList.get(i).getAnswerCount());
            sb.append(System.lineSeparator());
        }
        sb.append(System.lineSeparator());
        return  sb.toString();
    }
}