/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.concattweets;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author eve
 */
public class ConcatTweetsReducer extends Reducer<Text, BytesWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        //context.write(key, new IntWritable(sum));
        for(BytesWritable a : values)
        {
            this.writeToFile(a.toString(),key.toString());
        }
    }

    public void writeToFile(String line, String pathName) {
        try {
            Path pt = new Path("hdfs:output2/" + pathName);
            FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(pt)) {
                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.append(pt)));
                br.write(line);
                br.close();
            } else {
                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
                br.write(line);
                br.close();
            }
        } catch (Exception e) {
            System.out.println("File not found");
        }
    }

}
