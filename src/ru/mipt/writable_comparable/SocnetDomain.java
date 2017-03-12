package ru.mipt.writable_comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by dmitry on 12.03.17.
 */
public class SocnetDomain implements WritableComparable<SocnetDomain> {
    private Text socnet;
    private Text domain;


    public SocnetDomain(Text socnet, Text domain) {
        this.socnet = socnet;
        this.domain = domain;
    }

    public SocnetDomain(String socnet, String domain) {
        set(new Text(socnet), new Text(domain));
    }

    public void set(Text socnet, Text domain) {
        this.socnet = socnet;
        this.domain = domain;
    }

    @Override
    public int compareTo(SocnetDomain o) {
        int cmp = domain.compareTo(o.domain);
        if (cmp != 0) {
            return cmp;
        }
        return socnet.compareTo(o.socnet);
    }

    public Text getSocnet() {
        return socnet;
    }

    public Text getDomain() {
        return domain;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        socnet.write(dataOutput);
        domain.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        socnet.readFields(dataInput);
        domain.readFields(dataInput);
    }

    @Override
    public String toString() {
        return domain.toString() + "\t" + socnet.toString();
    }
}
