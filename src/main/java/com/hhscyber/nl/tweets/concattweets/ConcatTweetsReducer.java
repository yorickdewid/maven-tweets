/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.concattweets;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author eve
 */
public class ConcatTweetsReducer extends Reducer<Text, Text, Text, IntWritable> {

    private static BufferedWriter br = null;
    private FileSystem hdfs;
    private Path newFilePath;
    private StringBuilder sb;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        hdfs = FileSystem.get(context.getConfiguration());
        sb = new StringBuilder();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Path baseOutputPath = FileOutputFormat.getOutputPath(context);
        newFilePath = Path.mergePaths(baseOutputPath, new Path("/"+key + ".json"));
        try {
            for (Text value : values) {
                sb.append(value.toString());
                sb.append("\n");
            }
        } catch (Exception ex) {
            Logger.getLogger(ConcatTweetsReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        byte[] byt = sb.toString().getBytes();

        try (FSDataOutputStream fsOutStream = hdfs.create(newFilePath)) {
            fsOutStream.write(byt);
        }
    }
}
