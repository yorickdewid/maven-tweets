/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbase2;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author eve
 */
public class Hbase3 {
    private static Configuration conf;
    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args, "");
        Job job = new Job(conf, "hbasetest");
        job.setSpeculativeExecution(false);
        job.setMaxMapAttempts(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("input/1441737001"));//test one folder
        TextOutputFormat.setOutputPath(job, new Path("output3"));
        
        job.setMapperClass(Hbase2Mapper.class);
        job.setReducerClass(Hbase2Reducer.class);
        job.setCombinerClass(Hbase2Reducer.class);
        
        job.waitForCompletion(true);
    }
}
