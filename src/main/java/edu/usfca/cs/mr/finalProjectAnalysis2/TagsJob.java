package edu.usfca.cs.mr.finalProjectAnalysis2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TagsJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            if(args.length != 2){
                System.out.println("Usage(args len = 2) : <Input File Path> <Output File Path>");
                System.out.println("Exiting ... ");
                System.exit(1);
            }

            /* Job Name. You'll see this in the YARN webapp */
            Job job = Job.getInstance(conf, "word count job");
            /* Current class */
            job.setJarByClass(TagsJob.class);

            /* Mapper class */
            job.setMapperClass(TagsMapper.class);

            /* Combiner class. Combiners are run between the Map and Reduce
             * phases to reduce the amount of output that must be transmitted.
             * In some situations, we can actually use the Reducer as a Combiner
             * but ONLY if its inputs and ouputs match up correctly. The
             * combiner is disabled here, but the following can be uncommented
             * for this particular job: */
//            job.setCombinerClass(SentimentReducer.class);

            /* Reducer class */
            job.setReducerClass(TagsReducer.class);

            /* Outputs from the Mapper. */
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TagsWritable.class);

            /* Outputs from the Reducer */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(TagsWritable.class);

            /* Reduce tasks */
            job.setNumReduceTasks(1);

            /* Job input path in HDFS */
            FileInputFormat.addInputPath(job, new Path(args[0]));

            /* Job output path in HDFS. NOTE: if the output path already exists
             * and you try to create it, the job will fail. You may want to
             * automate the creation of new output directories here */
            FileOutputFormat.setOutputPath(job,  new Path(args[1]));

            /* Wait (block) for the job to complete... */
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
