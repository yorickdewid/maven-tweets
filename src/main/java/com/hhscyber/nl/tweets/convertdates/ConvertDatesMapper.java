/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.convertdates;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class ConvertDatesMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        Put p = resultToPut(row, value);
        if (p != null) {
            context.write(row, p);
        }
    }

    private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
        Put put = new Put(key.get());
        for (Cell c : result.rawCells()) {
            put.add(c);
        }
        byte[] b1 = hbasehelper.HbaseHelper.getValueSafe(result, "profile", "created_at");
        byte[] b2 = hbasehelper.HbaseHelper.getValueSafe(result, "content", "created_at");
        String profileCreatedAt = hbasehelper.HbaseHelper.createStringFromByte(b1);
        String contentCreatedAt = hbasehelper.HbaseHelper.createStringFromByte(b2);
        put.add(Bytes.toBytes("profile"), Bytes.toBytes("created_at_safe"), Bytes.toBytes(convertTextualToNumericDate(profileCreatedAt)));
        put.add(Bytes.toBytes("content"), Bytes.toBytes("created_at_safe"), Bytes.toBytes(convertTextualToNumericDate(contentCreatedAt)));
        return put;

    }

    private static String convertTextualToNumericDate(String dateText) {
        try {
            String indexes[] = dateText.split(" "); // creates 5 indexes starting on 0
            String month = indexes[1];//month textual
            String day = indexes[2]; //day numeric
            String year = indexes[5];//year; numeric
            Date date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(month);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            month = "0" + cal.get(Calendar.MONTH);
            
            return year + "-" + month + "-" + day;
        } catch (ParseException ex) {
            Logger.getLogger(ConvertDatesMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
}
