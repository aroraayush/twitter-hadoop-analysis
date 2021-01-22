package edu.usfca.cs.mr.sentiment_analysis;

import edu.usfca.cs.mr.util.MultiLineFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class SentimentAnalysisCountJob {

    public static void main(String[] args) {
        if(args.length < 5){
            System.out.println("Usage <input_file_path> <output_file_path_job_1> <output_file_path_job_2> <positive_word_txt_path> <negative_word_txt_path>");
            System.exit(0);
        }
        try {
            Configuration conf = new Configuration();
            /* Set up the positive_word_txt_path for mappers: */
            conf.setStrings("positive_word_txt_path", args[3]);
            /* Set up the negative_word_txt_path for mappers: */
            conf.setStrings("negative_word_txt_path", args[4]);

            // Not able to run 2 jobs in parallel in yarn, because it hangs. So, will run sequentially
            // https://community.cloudera.com/t5/Support-Questions/Yarn-applications-hang-foreever-if-run-in-parallel/td-p/15184

            /* ------------------------ Job 1 ---------------------------*/

            /* Job Name. You'll see this in the YARN webapp */
            Job job1 = Job.getInstance(conf, "Sentiment Analysis - Hashtag");

            // Ref: https://github.com/ElliottFodi/Hadoop-Programs/tree/master/Hadoop%20multiline%20read
            // Enables multiline reading of lines for the InputRecordReader
            // Reads 4 lines for our case
            job1.setInputFormatClass(MultiLineFormat.class);

            /* Current class */
            job1.setJarByClass(SentimentAnalysisCountJob.class);

            /* Mapper class */
            job1.setMapperClass(TagSAMapper.class);

            /* Reducer class */
            job1.setReducerClass(TagSAReducer.class);

            /* Outputs from the Mapper. */
            job1.setMapOutputKeyClass(IntWritable.class);
            job1.setMapOutputValueClass(WeekSAWritable.class);

            /* Outputs from the Reducer */
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(WeekSAWritable.class);

            /* Reduce tasks */
            job1.setNumReduceTasks(1);

            // Allowing the split size to unlimited / unbounded
            job1.getConfiguration().set("mapreduce.job.split.metainfo.maxsize", "-1");

            /* Job input path in HDFS */
            FileInputFormat.addInputPath(job1, new Path(args[0]));

            /* Job output path in HDFS. */
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));

            /* ------------------------ Job 2 ---------------------------*/

            /* Job Name */
            Job job2 = Job.getInstance(conf, "Sentiment Analysis - User");

            // Ref: https://github.com/ElliottFodi/Hadoop-Programs/tree/master/Hadoop%20multiline%20read
            // Enables multiline reading of lines for the InputRecordReader
            // Reads 4 lines for our case
            job2.setInputFormatClass(MultiLineFormat.class);

            /* Current class */
            job2.setJarByClass(SentimentAnalysisCountJob.class);

            // Ref: https://github.com/ElliottFodi/Hadoop-Programs/tree/master/Hadoop%20multiline%20read
            // Enables multiline reading of lines for the InputRecordReader
            // Reads 4 lines for our case
            job2.setInputFormatClass(MultiLineFormat.class);

            /* Current class */
            job2.setJarByClass(SentimentAnalysisCountJob.class);

            /* Mapper class */
            job2.setMapperClass(UserSAMapper.class);

            /* Reducer class */
            job2.setReducerClass(UserSAReducer.class);

            /* Outputs from the Mapper. */
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(UserSentimentWritable.class);

            /* Outputs from the Reducer */
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(UserSentimentWritable.class);

            /* Reduce tasks */
            job2.setNumReduceTasks(1);

            // Allowing the split size to unlimited / unbounded
            job2.getConfiguration().set("mapreduce.job2.split.metainfo.maxsize", "-1");

            /* Job input path in HDFS */
            FileInputFormat.addInputPath(job2, new Path(args[0]));

            /* Job output path in HDFS. NOTE: if the output path already exists
             * and you try to create it, the job2 will fail. You may want to
             * automate the creation of new output directories here */
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));


            /* ------------------ Running each job individually ---------------------*/
            /* Wait (block) for the job to complete... */
            //  System.exit(job1.waitForCompletion(true) ? 0 : 1);
            //  System.exit(job2.waitForCompletion(true) ? 0 : 1);

            /* ------------------ Running both jobs sequentially ---------------------*/

            if(job1.waitForCompletion(true)) {
                /* Wait (block) for the job2 to complete... */
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
