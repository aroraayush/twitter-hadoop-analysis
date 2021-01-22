package edu.usfca.cs.mr.finalProjectAnalysis1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommentsWritable implements Writable{

    // this custom writable is to analyze the comments data from stack overflow
    // it has post Id as the key, comment text, dates, scores, and other data related to the comments
    // also calculating the sentiment associated with the comments on stack overflow

    private IntWritable postId;
    private List<Text> commentText;
    private List<Text> date;
    private List<IntWritable> score;
    private List<IntWritable> userId;
    private DoubleWritable sentiment;
    private IntWritable commentsCount;


    public CommentsWritable(){
        this.postId = new IntWritable(0);
        this.commentText = new ArrayList<>();
        this.date = new ArrayList<>();
        this.score = new ArrayList<>();
        this.userId = new ArrayList<>();
        this.sentiment = new DoubleWritable(0.0);
        this.commentsCount = new IntWritable(0);
    }

    public CommentsWritable(int postId, List<Text> commentText, List<Text> date, List<IntWritable> score, List<IntWritable> userId, double sentiment, int count){
        this.postId = new IntWritable(postId);
        this.commentText = new ArrayList<>();
        this.commentText.addAll(commentText);
        this.date = new ArrayList<>();
        this.date.addAll(date);
        this.score = new ArrayList<>();
        this.score.addAll(score);
        this.userId = new ArrayList<>();
        this.userId.addAll(userId);
        this.sentiment = new DoubleWritable(sentiment);
        this.commentsCount = new IntWritable(count);
    }

    public IntWritable getPostId() {
        return postId;
    }

    public void setPostId(int postId) {
        this.postId = new IntWritable(postId);
    }

    public List<Text> getCommentText() {
        return commentText;
    }

    public void setCommentText(List<Text> commentText) {
        this.commentText.addAll(commentText);
    }

    public List<Text> getDate() {
        return date;
    }

    public void setDate(List<Text> date) {
        this.date.addAll(date);
    }

    public List<IntWritable> getScore() {
        return score;
    }

    public void setScore(List<IntWritable> score) {
        this.score.addAll(score);
    }

    public List<IntWritable> getUserId() {
        return userId;
    }

    public void setUserId(List<IntWritable> userId) {
        this.userId.addAll(userId);
    }

    public DoubleWritable getSentiment() {
        return sentiment;
    }

    public void setSentiment(double sentiment) {
        this.sentiment = new DoubleWritable(sentiment);
    }

    public IntWritable getCommentsCount() {
        return commentsCount;
    }

    public void setCommentsCount(int commentsCount) {
        this.commentsCount = new IntWritable(this.commentsCount.get() + commentsCount);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.postId.write(dataOutput);
        this.commentsCount.write(dataOutput);
        this.sentiment.write(dataOutput);
        dataOutput.writeInt(commentText.size());
        for(int x = 0;x < commentText.size();x++){
            this.commentText.get(x).write(dataOutput);
        }
        dataOutput.writeInt(date.size());
        for(int x = 0;x < date.size();x++){
            this.date.get(x).write(dataOutput);
        }
        dataOutput.writeInt(score.size());
        for(int x = 0;x < score.size();x++) {
            this.score.get(x).write(dataOutput);
        }
        dataOutput.writeInt(userId.size());
        for(int x = 0;x < userId.size();x++) {
            this.userId.get(x).write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.postId.readFields(dataInput);
        this.commentsCount.readFields(dataInput);
        this.sentiment.readFields(dataInput);
        int size = dataInput.readInt();
        commentText = new ArrayList<>(size);
        for(int x = 0;x < size;x++){
            Text t = new Text();
            t.readFields(dataInput);
            commentText.add(t);
        }
        size = dataInput.readInt();
        date = new ArrayList<>(size);
        for(int x = 0;x < size;x++){
            Text t = new Text();
            t.readFields(dataInput);
            date.add(t);
        }
        size = dataInput.readInt();
        score = new ArrayList<>(size);
        for(int x = 0;x < size;x++) {
            IntWritable t = new IntWritable();
            t.readFields(dataInput);
            score.add(t);
        }
        size = dataInput.readInt();
        userId = new ArrayList<>(size);
        for(int x = 0;x < size;x++) {
            IntWritable t = new IntWritable();
            t.readFields(dataInput);
            userId.add(t);
        }
    }

    @Override
    public String toString() {
        return "TagsWritable{" +
                "postId=" + postId +
                ", commentText=" + commentText +
                ", date=" + date +
                ", score=" + score +
                ", userId=" + userId +
                ", sentiment=" + sentiment +
                ", commentsCount=" + commentsCount +
                '}';
    }
}
