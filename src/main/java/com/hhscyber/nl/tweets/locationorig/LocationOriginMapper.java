/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.locationorig;

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
public class LocationOriginMapper extends TableMapper<ImmutableBytesWritable, Put> {

    //TODO
    // SPLIT INTO TWO REDUCERS EN OR NL
    // THEN DO THE 3 options and save to
    // impact:company
    // impact:number
    // impact:number_unit
    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        // divide to 10 reducers bases on non spam set
        long rowkey = Long.valueOf(new String(value.getRow()));
        // 59,408,8 
           /*
         Current count: 59409, row: 635632397812297728                                                                                                                                                                                                 
         Current count: 118818, row: 637482098564141056                                                                                                                                                                                                
         Current count: 178227, row: 639171446187495424                                                                                                                                                                                                
         Current count: 237636, row: 640762188878098436                                                                                                                                                                                                
         Current count: 297045, row: 642180411196604417                                                                                                                                                                                                
         Current count: 356454, row: 647108017150906368                                                                                                                                                                                                
         Current count: 415863, row: 648735861752795136                                                                                                                                                                                                
         Current count: 475272, row: 650036291338379264                                                                                                                                                                                                
         Current count: 534681, row: 651641578088435712
         */
        int reducer = 0 ;
        if (rowkey <= 635632397812297728l) {
            reducer = 1;
        } else if (rowkey >= 635632397812297728l && rowkey <= 637482098564141056l) {
            reducer = 2;
        } else if (rowkey >= 637482098564141056l && rowkey <= 639171446187495424l) {
            reducer = 3;
        } else if (rowkey >= 639171446187495424l && rowkey <= 640762188878098436l) {
            reducer = 4;
        } else if (rowkey >= 640762188878098436l && rowkey <= 642180411196604417l) {
            reducer = 5;
        } else if (rowkey >= 642180411196604417l && rowkey <= 647108017150906368l) {
            reducer = 6;
        } else if (rowkey >= 647108017150906368l && rowkey <= 648735861752795136l) {
            reducer = 7;
        } else if (rowkey >= 648735861752795136l && rowkey <= 650036291338379264l) {
            reducer = 8;
        } else if (rowkey >= 650036291338379264l && rowkey <= 651641578088435712l) {
            reducer = 9;
        } else if (rowkey >= 651641578088435712l) {
            reducer = 10;
        }
        context.write(new ImmutableBytesWritable(String.valueOf(reducer).getBytes()), value);
    }

}
