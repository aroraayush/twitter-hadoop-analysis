package edu.usfca.cs.mr.util;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/*
 *This class assigns the custom record reader to read two lines
 *as the record reader to be used
 */


public class MultiLineFormat extends TextInputFormat {

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new MultilineRecordReader();
    }
}