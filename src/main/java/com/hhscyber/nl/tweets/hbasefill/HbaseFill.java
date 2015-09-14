/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbasefill;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author eve
 */
public class HbaseFill {

    public final static String test = "1441737001";
    public static Configuration conHbase = null;
    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        conHbase = HBaseConfiguration.create();
        Job client = new Job(conHbase); // new configuration
        client.setJarByClass(HbaseFill.class);
        client.setOutputKeyClass(Text.class);
        client.setOutputValueClass(IntWritable.class);
        client.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(client, new Path("input/" + test));//test one folder
        TextOutputFormat.setOutputPath(client, new Path("output3"));

        client.setMapperClass(HbaseFillMapper.class);

        try {
            client.submit();
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
