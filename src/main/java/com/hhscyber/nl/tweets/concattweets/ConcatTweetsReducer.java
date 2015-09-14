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
import org.apache.hadoop.fs.FSDataInputStream;
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

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        BufferedWriter br = null;
        try {
            br = this.getFile(key.toString());
            for (Text value : values) {
                this.writeToFile(br, value.toString(), key.toString());
            }
            br.close();
        } catch (Exception ex) {
            Logger.getLogger(ConcatTweetsReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public BufferedWriter getFile(String pathName) throws Exception {
        try {
            Path pt = this.getPath(pathName);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br = null;
            if (!fs.exists(pt)) {
                br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
            } else {
                FSDataInputStream fsdata = fs.open(pt);
                br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
                br.write(fsdata.read());
            }
            return br;
        } catch (IOException ex) {
            Logger.getLogger(ConcatTweetsReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public void writeToFile(BufferedWriter br, String line, String pathName) {
        try {
            br.write(line);
        } catch (Exception e) {
            System.out.println("File not found");
        }
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
