package ru.mipt.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.mipt.writable_comparable.SocnetCount;

import java.io.IOException;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlSortReducer extends Reducer<SocnetCount, Text, Text, IntWritable> {
    @Override
    protected void reduce(SocnetCount key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text domain : values) {
            String line = key.getSocnet().toString() + "\t" + domain.toString();
            context.write(new Text(line), key.getCount());
        }
    }
}
