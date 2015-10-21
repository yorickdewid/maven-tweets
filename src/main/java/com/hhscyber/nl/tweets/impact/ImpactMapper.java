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

    

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        String words = HbaseHelper.createStringFromRawHbase(value, "content", "text");

        TextInspectorCompany company = new TextInspectorCompany(words);
        TextInspectorNumbers numbers = new TextInspectorNumbers(words);
        TextInspectorNumbersAndUnit numbersUnit = new TextInspectorNumbersAndUnit(words);
        if(company.getFoundWord() != null &&  numbers.getFoundWord() != null)
        {
            System.out.println("Company: "+company.getFoundWord() + " numbers: "+ numbers.getFoundWord() +" number and unit : "+numbersUnit.getFoundWord() + " " + numbersUnit.getUnit());
        }
    }


}
