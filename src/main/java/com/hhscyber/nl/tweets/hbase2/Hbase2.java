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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author eve
 */
public class Hbase2 implements Tool{
    private static Configuration conf;
    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        ToolRunner.run(new Configuration(),new Hbase2(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job client = new Job(getConf(),"hbasetest");
        client.setSpeculativeExecution(false);
        client.setMaxMapAttempts(2);
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
            client.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.out.println(e);
        }
        return 0;
    }

    @Override
    public void setConf(Configuration conf) {
        Hbase2.conf=conf;
    }

    @Override
    public Configuration getConf() {
         return conf;
    }
    
}
