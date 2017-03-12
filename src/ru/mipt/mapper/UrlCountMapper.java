package ru.mipt.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.mipt.writable_comparable.SocnetDomain;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by dmitry on 12.03.17.
 */
public class UrlCountMapper extends Mapper<LongWritable, Text, SocnetDomain, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String domain = getDomainName(fields[0]);
        String[] socnets = fields[1].split(";");
        for (String socnet : socnets) {
            String[] counts = socnet.split(":");
            context.write(new SocnetDomain(counts[0], domain), new IntWritable(Integer.valueOf(counts[1])));
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
