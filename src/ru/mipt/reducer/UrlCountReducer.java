package ru.mipt.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import ru.mipt.entity.VisitedDomain;
import ru.mipt.writable_comparable.SocnetDomain;

import java.io.IOException;
import java.util.*;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlCountReducer extends Reducer<SocnetDomain, IntWritable, SocnetDomain, IntWritable> {
    private static volatile Map<String, List<VisitedDomain>> map = new HashMap<>(10);
    private static final String[] RESULT_ORDER = new String[]{"vk", "facebook", "odnoklassniki", "twitter"};
    private static final Set<String> RESULT_SOCNETS = new HashSet<>();

    static {
        RESULT_SOCNETS.add("vk");
        RESULT_SOCNETS.add("facebook");
        RESULT_SOCNETS.add("odnoklassniki");
        RESULT_SOCNETS.add("twitter");
    }

    @Override
    public void reduce(SocnetDomain key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //context.write(key, new IntWritable(sum));
        String socnet = key.getSocnet().toString();
        synchronized (socnet) {
            if (map.containsKey(socnet)) {
                List<VisitedDomain> visitedDomains = map.get(socnet);
                visitedDomains.add(new VisitedDomain(key.getDomain().toString(), sum));
                if (visitedDomains.size() > 10) {
                    Collections.sort(visitedDomains, new Comparator<VisitedDomain>() {
                        @Override
                        public int compare(VisitedDomain o1, VisitedDomain o2) {
                            return -Integer.compare(o1.getCount(), o2.getCount());
                        }
                    });
                    visitedDomains.remove(visitedDomains.size() - 1);
                }
            }
            else {
                ArrayList<VisitedDomain> visitedDomainsList = new ArrayList<>();
                map.put(socnet, visitedDomainsList);
                visitedDomainsList.add(new VisitedDomain(key.getDomain().toString(), sum));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String key : RESULT_ORDER) {
            for (VisitedDomain visitedDomain : map.get(key)) {
                context.write(new SocnetDomain(key, visitedDomain.getDomain()), new IntWritable(visitedDomain.getCount()));
            }
        }
        for (String key : map.keySet()) {
            if (!RESULT_SOCNETS.contains(key)) {
                for (VisitedDomain visitedDomain : map.get(key)) {
                    context.write(new SocnetDomain(key, visitedDomain.getDomain()), new IntWritable(visitedDomain.getCount()));
                }
            }
        }
    }
}
