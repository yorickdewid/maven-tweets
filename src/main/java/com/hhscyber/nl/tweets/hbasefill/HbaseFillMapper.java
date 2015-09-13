/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbasefill;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
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
public class HbaseFillMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
    
    @Override
    public void map(LongWritable key, Text val,Context context) throws IOException, InterruptedException {
        this.connectHbase();
    }
    
    private void connectHbase(){
       Configuration config = HBaseConfiguration.create();

        try {
            // Instantiating HTable class
            HTable hTable = new HTable(config, "hhscyber:tweets");
        } catch (IOException ex) {
            Logger.getLogger(HbaseFillMapper.class.getName()).log(Level.SEVERE, null, ex);
        }

        JsonParse jp = new JsonParse();
        jp.openFileTest("input/"+HbaseFill.test);
    }
}
