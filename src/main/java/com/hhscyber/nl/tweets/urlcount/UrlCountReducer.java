package com.hhscyber.nl.tweets.urlcount;

import hbasehelper.HbaseHelper;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author dev
 */
public class UrlCountReducer extends Reducer<ImmutableBytesWritable, Result, Text, Text> {

    static int count = 0;
    static int countUrls = 0;

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        for (Result value : values) {
            count++;
            if (!HbaseHelper.createStringFromRawHbase(value, "content", "urls").equals("")) {
                countUrls++;
            }
        }
        context.write(new Text("Number records "),new Text(Integer.toString(count)));
        context.write(new Text("Number of urls "),new Text(Integer.toString(countUrls)));

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Number of record containing urls : " + countUrls);
        super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
    }

}
