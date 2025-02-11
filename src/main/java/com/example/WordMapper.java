package com.example;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Convert the line of text into a string
        String line = value.toString();

        // Split the line into words
        String[] words = line.split("\\s+");

        // Emit each word with a count of 1
        for (String w : words) {
            if (!w.isEmpty()) {
                word.set(w); // Set the word as the key
                context.write(word, one); // Emit (word, 1)
            }
        }
    }
}
