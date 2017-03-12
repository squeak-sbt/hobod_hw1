package ru.mipt.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import ru.mipt.writable_comparable.DomainCount;
import ru.mipt.writable_comparable.SocnetCount;

/**
 * Created by dmitry on 12.03.17.
 */
public class SocnetCountPartitioner extends Partitioner<SocnetCount, DomainCount> {
    @Override
    public int getPartition(SocnetCount socnetCount, DomainCount count, int i) {
        return (socnetCount.getSocnet().hashCode() & Integer.MAX_VALUE) % i;
    }
}
