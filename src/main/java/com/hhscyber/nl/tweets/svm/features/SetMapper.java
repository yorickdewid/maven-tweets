/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.features;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eve
 */
public class SetMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private Text word = new Text();

    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        byte[] data = value.getValue(Bytes.toBytes("content"), Bytes.toBytes("text"));

        StringTokenizer itr = new StringTokenizer(new String(data));
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, new IntWritable(1));
        }
    }

}
