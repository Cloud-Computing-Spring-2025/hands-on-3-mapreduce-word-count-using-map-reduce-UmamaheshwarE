
package com.example.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example.WordMapper;
import com.example.WordReducer;

public class Controller {

    public static void main(String[] args) throws Exception {
        // Ensure correct usage
        if (args.length != 2) {
            System.err.println("Usage: Controller <input path> <output path>");
            System.exit(-1);
        }

        // Set up the Hadoop job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");

        // Set the main driver class
        job.setJarByClass(Controller.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Specify input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and exit with success/failure code
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
