package edu.usfca.cs.mr.stack_overflow_highest_rep_users;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * It contains the location, total count of users at this location,
 * list of users at this location
 */
public class LocationWritable implements Writable {

    private Text location;
    private IntWritable count;
    private List<RepUserWritable> userList;

    //default constructor for (de)serialization
    public LocationWritable() {
        this.count = new IntWritable(0);
        this.location = new Text("");
        this.userList = new ArrayList<>();
    }

    public LocationWritable(int count, Text location, List<RepUserWritable> userList) {
        this.count = new IntWritable(count);
        this.location = new Text(location);
        this.userList = new ArrayList<>();
        this.userList.addAll(userList);
    }

    public void write(DataOutput out) throws IOException {
        // Serializing the data to send to next machine
        this.count.write(out);
        this.location.write(out);
        out.writeInt(userList.size());

        for(int index=0;index<userList.size();index++){
            // Serializing every values in list to send to next machine
            userList.get(index).write(out); //write all the value of list
        }
    }

    public void readFields(DataInput in) throws IOException {
        // deserializing
        this.count.readFields(in);
        this.location.readFields(in);

        int size = in.readInt(); //read size of list
        userList = new ArrayList<>(size);

        for(int i=0;i<size;i++){ //read all the values of list
            RepUserWritable hw = new RepUserWritable();
            hw.readFields(in);
            userList.add(hw);
        }
    }



    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = new IntWritable(this.count.get()+count.get());
    }

    public void addAlluserList(List<RepUserWritable> userList) {
        this.userList.addAll(userList);
    }

    public Text getLocation() {
        return location;
    }

    public void setLocation(Text location) {
        this.location = location;
    }

    public List<RepUserWritable> getUserList() {
        return userList;
    }

    public void setUserList(List<RepUserWritable> userList) {
        this.userList = userList;
    }

    @Override
    public String toString() {
        return "Total Users :" + count +
                System.lineSeparator() +
                userList +
                System.lineSeparator()
                ;
    }
}