/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.concattweets;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author eve
 */
public class ConcatTweetsMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {

    @Override
    public void map(LongWritable key, Text val,Context context) throws IOException, InterruptedException {
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        Text timestamp = new Text(this.getTimestampFromPath(filePathString));

        String line = val.toString();
        StringTokenizer itr = new StringTokenizer(line);
        while(itr.hasMoreTokens()) {
            // use timestamp as key
            context.write(timestamp, new BytesWritable(itr.nextToken().getBytes()));
        }        
    }
    
    public String getTimestampFromPath(String path){
        String timestamp = null;
        String[] tmp = path.split("/");
        for(String tm : tmp)
        {
            if(tm .matches("[0-9]*")){
              timestamp = tm;
              break;
            }
        }
        return timestamp;
    }
}
