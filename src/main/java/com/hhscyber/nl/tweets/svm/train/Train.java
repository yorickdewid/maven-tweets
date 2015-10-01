/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.train;

import io.github.htools.hadoop.Conf;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author eve
 */
public class Train {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        
        Conf conf = new Conf();
        //conf.set("outputpath", "trainer");
        
        
        Job client = new Job(conf, "SVMTrainer");
        client.setJarByClass(Train.class);
        client.setMapSpeculativeExecution(true);
        client.setReduceSpeculativeExecution(false);
        //client.setMaxMapAttempts(1);
        client.setMapOutputKeyClass(Text.class);
        client.setMapOutputValueClass(Text.class);
        
        client.setInputFormatClass(TextInputFormat.class);
        
        TextInputFormat.addInputPath(client, new Path("svmclass"));
        //client.setNumReduceTasks(countReducers(conf, inputPath));
        client.setNumReduceTasks(1);
        
        client.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(client, new Path("trainer"));
        //client.setOutputFormatClass(NullOutputFormat.class);
        // schrijf eventuele parameters in je configuration
        client.setMapperClass(TrainMapper.class);
        client.setReducerClass(TrainReducer.class);

        //hdfs.delete(new Path("jsonconcat"), true);
        // over het algemeen krijg je toch wel een stacktrace als iets mis gaat
        // dus je kunt throwen ipv afvangen
        try {
            client.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
