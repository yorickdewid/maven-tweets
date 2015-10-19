/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.test;

import io.github.htools.hadoop.Conf;
import io.github.htools.hbase.HBJob;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author eve
 */
public class Tester {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args, "");
        Job job = new HBJob(conf, "TweetsSVMTester");

        job.setJarByClass(Tester.class);
        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets_lang", scan, TestMapper.class, ImmutableBytesWritable.class, Put.class, job);

        //TableMapReduceUtil.initTableReducerJob("hhscyber:tweets_filtered", SetReducer.class, job);
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        //job.setMapperClass(MyMapper.class);
        job.setReducerClass(TestReducer.class);
        job.setNumReduceTasks(2);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());

        //job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }
}
