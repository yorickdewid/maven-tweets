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
    Text timestamp;
    
    @Override
    public void setup(Context context) {
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        timestamp = new Text(this.getTimestampFromPath(filePathString));
    }
    
    @Override
    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        
        String jsar = "{\"data\":"+val+",\"class\":\""+this.getKeyword(filePathString)+"\"}";
        context.write(timestamp, new Text(jsar));
    }

    public String getTimestampFromPath(String path) {
        String[] tmp = path.split("/");
        if (tmp[5] != null) {
            return tmp[6];
        } else {
            return null;
        }
    }
    
    public String getKeyword(String path) {
        String[] tmp = path.split("/");
        if (tmp[6] != null) {
            return tmp[7].substring(0, tmp[7].lastIndexOf('.'));
        } else {
            return null;
        }
    }
}
