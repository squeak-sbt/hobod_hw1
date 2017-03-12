package ru.mipt.writable_comparable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by dmitry on 12.03.17.
 */
public class DomainCount implements WritableComparable<DomainCount> {
    private Text domain;
    private IntWritable count;

    public DomainCount() {
        set(new Text(), new IntWritable());
    }

    public DomainCount(Text domain, IntWritable count) {
        this.domain = domain;
        this.count = count;
    }

    public DomainCount(String domain, int count){
        set(new Text(domain), new IntWritable(count));
    }

    public void set(Text domain, IntWritable count){
        this.domain = domain;
        this.count = count;
    }

    @Override
    public int compareTo(DomainCount o) {
        return domain.compareTo(o.domain);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        domain.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        domain.readFields(dataInput);
        count.readFields(dataInput);
    }

    public Text getDomain() {
        return domain;
    }

    public IntWritable getCount() {
        return count;
    }
}
