/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.linkcybertweets;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.DirComponent;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

/**
 *
 * @author eve
 */
public class LinkCyberTweets {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args, "");
        Job job = new Job(conf, "LinkCyberTweets");
        job.setJarByClass(LinkCyberTweets.class);
        String stop = "633916417071407104"; //50000 tweets
        Scan scan = new Scan();
        scan.setStopRow(stop.getBytes());
        
        List<Filter> rowFilters = new ArrayList<>();
        SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(hbasehelper.HbaseHelper.getPutBytesSafe("content"),
                hbasehelper.HbaseHelper.getPutBytesSafe("created_at"),
                CompareFilter.CompareOp.EQUAL,
                hbasehelper.HbaseHelper.getPutBytesSafe("2015-08-19"));
        rowFilters.add(filterColumn);
        
        FilterList filters = new FilterList(rowFilters);

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets_test", scan, LinkCyberTweetsMapper.class, null, null, job);
        job.setNumReduceTasks(0);

        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists("hhscyber:tweets_link_test")) {
            HTableDescriptor htable = new HTableDescriptor(("hhscyber:tweets_link_test"));
            htable.addFamily(new HColumnDescriptor("content"));
            htable.addFamily(new HColumnDescriptor("profile"));
            htable.addFamily(new HColumnDescriptor("link"));
            admin.createTable(htable);
        }
        TableMapReduceUtil.initTableReducerJob("hhscyber:tweets_link_test", null, job); // if disabled no output folder specfied exception

        job.waitForCompletion(true);
    }

    public static int countReducers(Configuration conf, Path inputPath) throws IOException {
        HashSet<String> timestamps = new HashSet();
        HDFSPath inHdfsPath = new HDFSPath(conf, inputPath);
        for (DirComponent path : inHdfsPath.wildcardIterator()) {
            timestamps.add(path.getName());
        }
        return timestamps.size();
    }

}
