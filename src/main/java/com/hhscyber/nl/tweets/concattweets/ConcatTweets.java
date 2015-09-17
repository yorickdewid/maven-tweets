/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.concattweets;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author eve
 */
public class ConcatTweets {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        Job client = new Job(conf);
        client.setSpeculativeExecution(false);
        client.setJarByClass(ConcatTweets.class);
        client.setJobName("com.hhscyber.nl.tweets.concattweets.ConcatTweets");
        //client.setMaxMapAttempts(1);
        //client.setMaxReduceAttempts(1);
        client.setOutputKeyClass(Text.class);
        client.setOutputValueClass(Text.class);
        client.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(client, new Path("input/143*")); //1440* ...
        TextOutputFormat.setOutputPath(client, new Path("jsonconcat"));

        client.setMapperClass(ConcatTweetsMapper.class);
        client.setReducerClass(ConcatTweetsReducer.class);

        FileSystem hdfs = FileSystem.get(conf);
        hdfs.delete(new Path("jsonconcat"), true);

        try {
            client.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
