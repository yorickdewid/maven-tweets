/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbasefill;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author eve
 */
public class HbaseFillMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
    
    @Override
    public void map(LongWritable key, Text val,Context context) throws IOException, InterruptedException {
        this.parseJson();
    }
    
    private void parseJson(){
        JsonParse jp = new JsonParse();
        jp.openFileTest("input/"+HbaseFill.test);
    }
    
}
