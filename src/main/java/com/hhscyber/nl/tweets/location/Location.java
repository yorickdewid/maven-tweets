/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
    package com.hhscyber.nl.tweets.location;

import com.hhscyber.nl.tweets.lang.*;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.DirComponent;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author eve
 */
public class Location {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf();
        Job job = new Job(conf, "TweetsLocation");
        String stop = "633223982884327426"; //1000 tweets?
        Scan scan = new Scan();
        scan.setStopRow(stop.getBytes());

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets", scan, LocationMapper.class, null, null, job);
        job.setNumReduceTasks(0);

        //TableMapReduceUtil.initTableReducerJob("hhscyber:tweets_lang", null, job);

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
