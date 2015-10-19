/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.test;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eve
 */
public class TestMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private Text word = new Text();

    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        Put put = new Put(row.get());
        for (KeyValue kv : value.raw()) {
            put.add(kv);
        }
        context.write(row, put);
    }
}
