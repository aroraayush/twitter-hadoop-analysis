package edu.usfca.cs.mr.advanced_analysis_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Custom writable to store data for the week
 * Contains week number, total count of user mentions in the week
 * Popular user's for the week
 */
public class WeeklyUsersWritable implements Writable {

    private IntWritable week;   // week number
    private IntWritable count;  //  total count of popular users in the week
    private List<UsersWritable> usersList;  // List of popular user's

    //default constructor for (de)serialization
    public WeeklyUsersWritable() {
        this.count = new IntWritable(0);
        this.week = new IntWritable(0);
        this.usersList = new ArrayList<>();
    }

    // Constructor for storing data for mapper
    public WeeklyUsersWritable(int count, int week, List<UsersWritable> usersList) {
        this.count = new IntWritable(count);
        this.week = new IntWritable(week);
        this.usersList = new ArrayList<>();
        this.usersList.addAll(usersList);
    }

    // Serializing the data to send to next machine
    public void write(DataOutput out) throws IOException {
        this.count.write(out);      // Serializing the count
        this.week.write(out);       // Serializing the week number
        out.writeInt(usersList.size());     // Serializing the length of users

        for(int index=0;index<usersList.size();index++){
            // Serializing every values in list to send to next machine
            usersList.get(index).write(out); //write all the value of list
        }
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.count.readFields(in);
        this.week.readFields(in);

        int size = in.readInt(); //read size of users list
        usersList = new ArrayList<>(size);

        for(int i = 0 ; i < size ; i++){    //read all the values of users list
            UsersWritable hw = new UsersWritable();
            hw.readFields(in);
            usersList.add(hw);
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

    public List<UsersWritable> getusersList() {
        return usersList;
    }

    public void setUsersList(List<UsersWritable> usersList) {
        this.usersList.addAll(usersList);
    }

    @Override
    public String toString() {
        return  ", count=" + count +
                ", Users " + usersList;
    }
}