/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.concattweets;

import com.google.common.collect.Iterables;
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

/**
 *
 * @author eve
 */
public class ConcatTweetsReducer extends Reducer<Text, Text, Text, IntWritable> {

    private static BufferedWriter br = null;
    private FileSystem hdfs;
    private Path newFolderPath;
    private Path newFilePath;
    private StringBuilder sb;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        hdfs = FileSystem.get(context.getConfiguration());
        newFilePath = new Path("newFile.json");

        sb = new StringBuilder();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("Reduce with "+key);
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

        FSDataOutputStream fsOutStream = hdfs.create(newFilePath);

        fsOutStream.write(byt);
        fsOutStream.close();
    }

    public BufferedWriter getFile(String pathName) throws Exception {
        try {
            Path pt = this.getPath(pathName);
            FileSystem fs = FileSystem.get(new Configuration());
            br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
            return br;
        } catch (IOException ex) {
            Logger.getLogger(ConcatTweetsReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private Path getPath(String pathName) {
        Path pt = null;
        if (pathName.length() == 0 || pathName == null) {
            pt = new Path(1441737001 + ".json");
        } else {
            pt = new Path(pathName + ".json");
        }
        return pt;
    }

}
