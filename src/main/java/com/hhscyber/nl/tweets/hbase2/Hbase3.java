/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbase2;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.DirComponent;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.mapreduce.*;

/**
 *
 * @author eve
 */
public class Hbase3 {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args, "input");
        Job job = new Job(conf, "TweetsHbase3");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReduceSpeculativeExecution(true);

        job.setInputFormatClass(TextInputFormat.class);

        TableMapReduceUtil.initTableReducerJob("hhscyber:tweets_test2", null, job);

        job.setMapperClass(Hbase2Mapper.class);
        job.setReducerClass(Hbase2Reducer.class);
        job.setNumReduceTasks(countReducers(conf, conf.getHDFSPath("input")));

        TextInputFormat.addInputPath(job, conf.getHDFSPath("input"));

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
