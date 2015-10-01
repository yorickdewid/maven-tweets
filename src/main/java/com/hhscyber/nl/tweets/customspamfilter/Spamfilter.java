/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
    package com.hhscyber.nl.tweets.customspamfilter;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.DirComponent;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

/**
 *
 * @author eve
 */
public class Spamfilter {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf();
        Job job = new Job(conf, "TweetsLanguage");

        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets", scan, SpamfilterMapper.class, null, null, job);
        job.setNumReduceTasks(0);

        TableMapReduceUtil.initTableReducerJob("hhscyber:tweets_lang", null, job);

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
