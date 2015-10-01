/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.customspamfilter;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eve
 */
public class SpamfilterMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        Put p = resultToPut(row, value);
        if (p != null) {
            context.write(row, p);
        }
    }
    
    private static void filterText(ImmutableBytesWritable key, Result result){
        byte[] b = result.getValue(Bytes.toBytes("content"), Bytes.toBytes("text"));
        byte[] b2 = result.getValue(Bytes.toBytes("profile"), Bytes.toBytes("url"));
        String text = new String(b);
        String url = new String(b2);
        String[] words = text.split(" ");
        if(words.length < 2 && url == null )
        {
            //mark as spam
        }
    }

    private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
        Put put = new Put(key.get());
        byte[] b = result.getValue(Bytes.toBytes("content"), Bytes.toBytes("lang"));
        byte[] b2 = result.getValue(Bytes.toBytes("profile"), Bytes.toBytes("lang"));

        String clang = new String(b);
        String plang = new String(b2);

        System.out.println("CLANG " + clang + " PLANG " + plang);
        if (clang.equals("en") || plang.equals("en") || clang.equals("nl") || plang.equals("nl")) {
            for (Cell c: result.rawCells()) {
                put.add(c);
            }
            return put;
        } else {
            return null;
        }

    }
}
