/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.urlcount;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eve
 */
public class UrlCountMapper extends TableMapper<ImmutableBytesWritable, Result> {

    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        context.write(row,value);
    }
 
}
