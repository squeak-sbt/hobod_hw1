package ru.mipt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.mipt.comparator.SocnetCountGroupComparator;
import ru.mipt.mapper.UrlSortMapper;
import ru.mipt.partitioner.SocnetCountPartitioner;
import ru.mipt.reducer.UrlSortReducer;
import ru.mipt.writable_comparable.SocnetCount;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlSort extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new UrlSort(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job  = new Job(conf);
        job.setJarByClass(UrlSort.class);

        job.setMapperClass(UrlSortMapper.class);
        job.setReducerClass(UrlSortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //job.setSortComparatorClass(SocnetDomainComparator.class);
        job.setGroupingComparatorClass(SocnetCountGroupComparator.class);

        job.setPartitionerClass(SocnetCountPartitioner.class);

        job.setMapOutputKeyClass(SocnetCount.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : -1;
    }
}
