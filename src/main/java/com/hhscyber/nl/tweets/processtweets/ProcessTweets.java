/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.processtweets;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author eve
 */
public class ProcessTweets {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {

        Job client = new Job(new Configuration());
        client.setJarByClass(ProcessTweets.class);
        client.setOutputKeyClass(Text.class);
        client.setOutputValueClass(IntWritable.class);
        client.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(client, new Path("input_concat"));//
        TextOutputFormat.setOutputPath(client, new Path("output2"));
        
        client.setMapperClass(ProcessTweetsMapper.class);
        client.setReducerClass(ProcessTweetsReducer.class);
        client.setCombinerClass(ProcessTweetsReducer.class);
        
        try {
            client.submit();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
}
