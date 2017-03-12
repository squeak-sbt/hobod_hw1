package ru.mipt.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import ru.mipt.writable_comparable.SocnetDomain;
import ru.mipt.writable_comparable.UrlWritable;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlGroupComparator extends WritableComparator {
    protected UrlGroupComparator() {
        super(SocnetDomain.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        SocnetDomain domain1 = (SocnetDomain)a;
        SocnetDomain domain2 = (SocnetDomain)b;
        return domain1.getSocnet().compareTo(domain2.getSocnet());
    }
}
