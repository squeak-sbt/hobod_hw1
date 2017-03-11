package ru.mipt.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String[] socnets = fields[1].split(";");
        for (String socnet : socnets) {
            String[] counts = socnet.split(":");
            Text text = new Text();
            text.set(counts[0]);
            context.write(text, new IntWritable(Integer.valueOf(counts[1])));
        }
    }
}
