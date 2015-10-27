/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
    package com.hhscyber.nl.tweets.locationorig;

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

/**
 *
 * @author eve
 */
public class LocationOrigin {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args,"");
        Job job = new Job(conf, "Location_origin");
        job.setJarByClass(LocationOrigin.class);
        String stop = "633223982884327426"; //1000 tweets?
        Scan scan = new Scan();
        job.setSpeculativeExecution(false);
        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets_location_test", scan, LocationOriginMapper.class, ImmutableBytesWritable.class, Result.class, job);
        job.setNumReduceTasks(10);

        TableMapReduceUtil.initTableReducerJob("hhscyber:tweets_location_test", LocationOriginReducer.class, job); // if disabled no output folder specfied exception

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
