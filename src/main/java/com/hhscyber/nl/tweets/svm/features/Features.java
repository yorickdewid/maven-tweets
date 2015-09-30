/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.features;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author eve
 */
public class Features {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf();
        Job job = new Job(conf, "TweetsSVMFeatureSet");

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("content"));

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets_test", scan, SetMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob("hhscyber:svm_featureset", SetReducer.class, job);
        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }
    
}
