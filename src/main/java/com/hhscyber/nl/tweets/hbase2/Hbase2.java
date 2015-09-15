/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbase2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author eve
 */
public class Hbase2 {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {

        Job client = new Job(new Configuration());
        client.setSpeculativeExecution(false);
        client.setJarByClass(Hbase2.class);
        client.setOutputKeyClass(Text.class);
        client.setOutputValueClass(Text.class);
        client.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(client, new Path("input/1441737001"));//test one folder
        TextOutputFormat.setOutputPath(client, new Path("output3"));
        
        client.setMapperClass(Hbase2Mapper.class);
        client.setReducerClass(Hbase2Reducer.class);
        client.setCombinerClass(Hbase2Reducer.class);
        
        try {
            client.submit();
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
        }
        
    }
    
}
