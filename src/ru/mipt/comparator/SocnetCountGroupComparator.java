package ru.mipt.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import ru.mipt.writable_comparable.SocnetCount;

/**
 * Created by dmitry on 12.03.17.
 */
public class SocnetCountGroupComparator extends WritableComparator {
    protected SocnetCountGroupComparator() {
        super(SocnetCount.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof SocnetCount && b instanceof SocnetCount) {
            return ((SocnetCount) a).getSocnet().toString().compareTo(((SocnetCount) b).getSocnet().toString());
        }
        return super.compare(a, b);
    }
}
