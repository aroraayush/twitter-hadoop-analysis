package edu.usfca.cs.mr.stack_overflow_top5_posts;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * It contains the meta data for the post (class variables)
 */
public class PostWritable implements Writable {

    private Text title;     // title of the post
    private DoubleWritable sentiment;   // sentiment of the post
    private IntWritable score;      // score for the post
    private IntWritable answerCount;       // answer count for the post
    private Text date;          // data for the post

    //default constructor for (de)serialization
    public PostWritable() {
        this.score = new IntWritable(0);
        this.answerCount = new IntWritable(0);
        this.sentiment = new DoubleWritable(0.0);
        this.title = new Text("");
        this.date = new Text("");
    }

    public PostWritable(double count, Text title,Text date, int score, int answerCount) {
        this.sentiment = new DoubleWritable(count);
        this.title = new Text(title);
        this.date = new Text(date);
        this.score = new IntWritable(score);
        this.answerCount = new IntWritable(answerCount);
    }

    public void write(DataOutput out) throws IOException {
        // Serializing the data to send to next machine
        this.sentiment.write(out);
        this.title.write(out);
        this.date.write(out);
        this.score.write(out);
        this.answerCount.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.sentiment.readFields(in);
        this.title.readFields(in);
        this.date.readFields(in);
        this.score.readFields(in);
        this.answerCount.readFields(in);
    }

    public Text getTitle() {
        return title;
    }

    public void setTitle(Text title) {
        this.title = title;
    }

    public DoubleWritable getSentiment() {
        return sentiment;
    }

    public void setSentiment(DoubleWritable sentiment) {
        this.sentiment = sentiment;
    }

    public IntWritable getScore() {
        return score;
    }

    public void setScore(IntWritable score) {
        this.score = score;
    }

    public Text getDate() {
        return date;
    }

    public void setDate(Text date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "Post{" +
                "title=" + title +
                ", sentiment=" + sentiment +
                ", score=" + score +
                ", answerCount=" + answerCount +
                ", date=" + date +
                '}';
    }

    public IntWritable getAnswerCount() {
        return answerCount;
    }
}