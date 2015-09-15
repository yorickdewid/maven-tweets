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

        hdfs = FileSystem.get(new Configuration());
        Path homeDir = hdfs.getHomeDirectory();
        Path workingDir = hdfs.getWorkingDirectory();

        System.out.println("Home folder -" + homeDir);
        System.out.println("Work folder -" + workingDir);

        newFolderPath = new Path("/MyDataFolder");
        newFolderPath = Path.mergePaths(workingDir, newFolderPath);

        hdfs.mkdirs(newFolderPath);

        newFilePath = new Path(newFolderPath + "/newFile.txt");
        hdfs.createNewFile(newFilePath);

        sb = new StringBuilder();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        try {
            //br = this.getFile(key.toString());
            //System.out.println("Size: " + Iterables.size(values));

            for (Text value : values) {
                //this.writeToFile(br, value.toString(), key.toString());
                //this.writeFile(value.toString());
                sb.append(value.toString());
                sb.append("\n");
            }
            //br.close();
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
