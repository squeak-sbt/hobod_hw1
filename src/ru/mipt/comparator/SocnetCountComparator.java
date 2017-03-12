package ru.mipt.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import ru.mipt.writable_comparable.SocnetCount;

/**
 * Created by dmitry on 12.03.17.
 */
public class SocnetCountComparator extends WritableComparator {

    protected SocnetCountComparator() {
        super(SocnetCount.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof SocnetCount && b instanceof SocnetCount) {
            return ((SocnetCount)a).getCount().compareTo(((SocnetCount)b).getCount());
        }
        return super.compare(a, b);
    }
}
