package ru.mipt.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.mipt.writable_comparable.SocnetCount;

import java.io.IOException;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlSortMapper extends Mapper<LongWritable, Text, SocnetCount, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        context.write(new SocnetCount(new Text(fields[0]), new IntWritable(Integer.valueOf(fields[2]))), new Text(fields[1]));
    }
}
