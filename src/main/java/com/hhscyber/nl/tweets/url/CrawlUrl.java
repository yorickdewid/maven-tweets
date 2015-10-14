/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
    package com.hhscyber.nl.tweets.url;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.DirComponent;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

/**
 *
 * @author eve
 */
public class CrawlUrl {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        String outputTableName = "hhscyber:tweets_titles_test";
        Conf conf = new Conf(args,"");
        Job job = new Job(conf, "CrawlUrl");
        job.setJarByClass(CrawlUrl.class);
        String stop = "633218488010735617"; //475 tweets
        Scan scan = new Scan();
        scan.setStopRow(stop.getBytes());

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets_lang", scan, CrawlUrlMapper.class, ImmutableBytesWritable.class, Result.class, job);
        job.setNumReduceTasks(1);

        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(outputTableName)) {
            HTableDescriptor htable = new HTableDescriptor((outputTableName));
            htable.addFamily(new HColumnDescriptor("content"));
            admin.createTable(htable);
        }
        
        TableMapReduceUtil.initTableReducerJob(outputTableName, CrawlUrlReducer.class, job); // if disabled no output folder specfied exception

        
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
