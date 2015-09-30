/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.dataout;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author eve
 */
public class DataOutMapper extends TableMapper<Text, Text> {

    private Text textk = new Text();
    private Text textv = new Text();

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        String val = new String(value.getValue(Bytes.toBytes("content"), Bytes.toBytes("text")));
        String key = new String(row.get());
        textk.set(key);
        textv.set(val);

        context.write(textk, textv);
    }
}
