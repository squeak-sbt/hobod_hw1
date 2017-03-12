package ru.mipt.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import ru.mipt.writable_comparable.SocnetDomain;

/**
 * Created by dmitry on 12.03.17.
 */
public class SocnetDomainComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        SocnetDomain socnetDomain1 = (SocnetDomain)a;
        SocnetDomain socnetDomain2 = (SocnetDomain)b;
        return socnetDomain1.compareTo(socnetDomain2);
    }
}
