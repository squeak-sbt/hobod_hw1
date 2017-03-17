package ru.mipt.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.mipt.entity.VisitedDomain;
import ru.mipt.writable_comparable.SocnetDomain;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlCountMapper extends Mapper<LongWritable, Text, SocnetDomain, IntWritable> {
    private static volatile Map<String, List<VisitedDomain>> map = new HashMap<>(10);
    private static final String[] RESULT_ORDER = new String[]{"vk", "facebook", "odnoklassniki", "twitter"};

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String domain = getDomainName(fields[0]);
        String[] socnets = fields[1].split(";");
        for (String socnet : socnets) {
            String[] counts = socnet.split(":");
            //context.write(new SocnetDomain(counts[0], domain), new IntWritable(Integer.valueOf(counts[1])));
            int count = Integer.valueOf(counts[1]);
            synchronized (counts[0]) {
                if (map.containsKey(counts[0])) {
                    List<VisitedDomain> visitedDomains = map.get(counts[0]);
                    visitedDomains.add(new VisitedDomain(domain, count));
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
    }

    private String getDomainName(String url) throws MalformedURLException {
        if(!url.startsWith("http") && !url.startsWith("https")){
            url = "http://" + url;
        }
        URL netUrl = new URL(url);
        String host = netUrl.getHost();
        if(host.startsWith("www")){
            host = host.substring(4);
        }
        String[] domains = host.split("\\.");
        if (domains.length >= 2) {
            host = domains[domains.length - 2] + "." + domains[domains.length - 1];
        }
        return host;
    }
}
