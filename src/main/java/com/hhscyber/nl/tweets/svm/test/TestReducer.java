/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.test;

import static io.github.htools.lib.BoolTools.word;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author eve
 */
public class TestReducer extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put put : values) {
            List<Cell> cell = put.get(Bytes.toBytes("content"), Bytes.toBytes("text"));
            String data = Bytes.toString(CellUtil.cloneValue(cell.get(0)));
            if (isSpam(data)) {
                context.write(new ImmutableBytesWritable(Bytes.toBytes("hhscyber:tweets_spam")), put);
            } else {
                context.write(new ImmutableBytesWritable(Bytes.toBytes("hhscyber:tweets_filtered")), put);
            }
        }

    }

    private boolean isSpam(String data) {
        StringTokenizer itr = new StringTokenizer(data);
        while (itr.hasMoreTokens()) {
            //word.set(itr.nextToken());
            System.out.println("NEXT " + itr.nextToken());
            //context.write(word, new IntWritable(1));
        }
        Random rand = new Random();
        return rand.nextInt(1)==1;
    }
}