/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbase2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author eve
 */
public class Hbase2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        Text timestamp = new Text(this.getTimestampFromPath(filePathString));
        System.out.println("Filepath: " + filePathString + " " + timestamp);
        // use timestamp as key
        context.write(timestamp, val);
    }

    public String getTimestampFromPath(String path) {
        String[] tmp = path.split("/");
        if (tmp[5] != null) {
            return tmp[6];
        } else {
            return null;
        }
    }
}
