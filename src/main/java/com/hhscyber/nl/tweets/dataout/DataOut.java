/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.dataout;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author eve
 */
public class DataOut {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf();
        Job job = new Job(conf, "TweetsDataOut");

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("content"));

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets_test", scan, DataOutMapper.class, Text.class, Text.class, job);

        job.setReducerClass(DataOutReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path("test_data_out"));  // adjust directories as required
        
        job.waitForCompletion(true);
    }

}
