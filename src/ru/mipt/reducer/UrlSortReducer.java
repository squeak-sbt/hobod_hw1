package ru.mipt.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.mipt.writable_comparable.DomainCount;
import ru.mipt.writable_comparable.SocnetCount;

import java.io.IOException;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlSortReducer extends Reducer<SocnetCount, DomainCount, Text, IntWritable> {
    @Override
    protected void reduce(SocnetCount key, Iterable<DomainCount> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (DomainCount domain : values) {
            if (count >= 10) {
                break;
            }
            String line = key.getSocnet().toString() + "\t" + domain.getDomain().toString();
            context.write(new Text(line), domain.getCount());
            count++;
        }
        context.write(new Text("DEEEE"), new IntWritable(15));
    }
}
