package ru.mipt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.mipt.comparator.SocnetDomainComparator;
import ru.mipt.comparator.UrlGroupComparator;
import ru.mipt.mapper.UrlCountMapper;
import ru.mipt.partitioner.SocnetDomainPartitioner;
import ru.mipt.reducer.UrlCountReducer;
import ru.mipt.writable_comparable.SocnetDomain;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new UrlCount(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job  = new Job(conf);
        job.setJarByClass(UrlCount.class);

        job.setMapperClass(UrlCountMapper.class);
        job.setReducerClass(UrlCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setSortComparatorClass(SocnetDomainComparator.class);
        job.setGroupingComparatorClass(UrlGroupComparator.class);

        job.setPartitionerClass(SocnetDomainPartitioner.class);

        job.setMapOutputKeyClass(SocnetDomain.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : -1;
    }
}
