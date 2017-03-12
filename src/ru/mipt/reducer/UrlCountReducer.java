package ru.mipt.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.mipt.writable_comparable.SocnetDomain;

import java.io.IOException;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlCountReducer extends Reducer<SocnetDomain, IntWritable, SocnetDomain, IntWritable> {
    @Override
    public void reduce(SocnetDomain key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
