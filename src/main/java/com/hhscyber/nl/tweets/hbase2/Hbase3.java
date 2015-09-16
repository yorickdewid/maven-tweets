/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbase2;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author eve
 */
public class Hbase3 {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args, "input output");
        Job job = new Job(conf, "hbasetest");
        job.setMapSpeculativeExecution(true);
        job.setReduceSpeculativeExecution(false);
        job.setMaxMapAttempts(4);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        
        // gooit outpath naar .Trash toe, als het fout gaat kun je het daar vandaan terughalen
        HDFSPath outPath = conf.getHDFSPath("output");
        outPath.trash(); 
        TextOutputFormat.setOutputPath(job, outPath);
        TextInputFormat.addInputPath(job, conf.getHDFSPath("input")); // moet input/1441737001 zijn
        
        job.setMapperClass(Hbase2Mapper.class);
        job.setReducerClass(Hbase2Reducer.class);
        job.setCombinerClass(Hbase2Reducer.class);
        
        job.waitForCompletion(true);
    }
}
