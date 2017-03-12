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

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return super.compare(b1, s1, l1, b2, s2, l2);
    }

    @Override
    public int compare(Object a, Object b) {
        return super.compare(a, b);
    }
}
