/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.impact;

import hbasehelper.HbaseHelper;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eve
 */
public class ImpactMapper extends TableMapper<ImmutableBytesWritable, Put> {


    //TODO
    // SPLIT INTO TWO REDUCERS EN OR NL
    // THEN DO THE 3 options and save to
    // impact:company
    // impact:number
    // impact:number_unit
    
    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        String content_lang = HbaseHelper.createStringFromRawHbase(value, "content", "lang");
        String profile_lang = HbaseHelper.createStringFromRawHbase(value, "profile", "lang");
        
        if(content_lang.toLowerCase().contains("en") || profile_lang.toLowerCase().contains("en"))
        {
            context.write(new ImmutableBytesWritable("en".getBytes()),value);
        }
        else if(content_lang.toLowerCase().contains("nl") || profile_lang.toLowerCase().contains("nl"))
        {
            context.write(new ImmutableBytesWritable("nl".getBytes()),value);
        }
    }


}
