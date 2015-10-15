/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
    package com.hhscyber.nl.tweets.urlcount;

import com.hhscyber.nl.tweets.locationcount.*;
import com.hhscyber.nl.tweets.location.*;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.DirComponent;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author eve
 */
public class UrlCount {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args,"");
        Job job = new Job(conf, "CountUrlTweets");
        job.setJarByClass(UrlCount.class);
        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets_lang", scan, UrlCountMapper.class, ImmutableBytesWritable.class, Result.class, job);
        job.setNumReduceTasks(1);

        job.setReducerClass(UrlCountReducer.class);
        TextOutputFormat.setOutputPath(job, new Path("urlcount"));
        //TableMapReduceUtil.initTableReducerJob("hhscyber:tweets_location_test", LocationCountReducer.class, job); // if disabled no output folder specfied exception

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
