package ru.mipt.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import ru.mipt.writable_comparable.SocnetDomain;

/**
 * Created by dmitry on 12.03.17.
 */
public class SocnetDomainPartitioner extends Partitioner<SocnetDomain, IntWritable> {
    @Override
    public int getPartition(SocnetDomain socnetDomain, IntWritable intWritable, int i) {
        return (socnetDomain.getSocnet().hashCode() & Integer.MAX_VALUE) % i;
    }
}
