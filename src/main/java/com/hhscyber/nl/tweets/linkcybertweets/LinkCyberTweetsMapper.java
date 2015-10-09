/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.linkcybertweets;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eve
 */
public class LinkCyberTweetsMapper extends TableMapper<Text, Result> {
    
    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        byte[] b1 = hbasehelper.HbaseHelper.getValueSafe(value, "content", "created_at_safe");
        String created_at = hbasehelper.HbaseHelper.createStringFromByte(b1);
        context.write(created_at, value);
    }
}
