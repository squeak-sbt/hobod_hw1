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
public class SocnetCount implements WritableComparable<SocnetCount> {

    private Text socnet;
    private IntWritable count;

    public SocnetCount(Text socnet, IntWritable count) {
        this.socnet = socnet;
        this.count = count;
    }

    public SocnetCount(String socnet, int count) {
        set(new Text(socnet), new IntWritable(count));
    }

    public SocnetCount(){
        set(new Text(), new IntWritable());
    }

    public Text getSocnet() {
        return socnet;
    }

    public IntWritable getCount() {
        return count;
    }

    public void set(Text socnet, IntWritable count) {
        this.socnet = socnet;
        this.count = count;
    }

    @Override
    public int compareTo(SocnetCount o) {
        return -count.compareTo(o.count);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        socnet.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        socnet.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public String toString() {
        return socnet.toString() + "\t" + count.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SocnetCount that = (SocnetCount) o;

        return socnet != null ? socnet.toString().equals(that.socnet.toString()) : that.socnet == null;

    }

    @Override
    public int hashCode() {
        return socnet != null ? socnet.toString().hashCode() : 0;
    }
}
