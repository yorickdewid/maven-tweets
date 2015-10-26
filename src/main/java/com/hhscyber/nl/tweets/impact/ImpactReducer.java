/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.impact;

import hbasehelper.HbaseHelper;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author dev
 */
public class ImpactReducer extends TableReducer<ImmutableBytesWritable, Result, Put> {

    String path = "nlp_models/";
    static InputStream modelIn = null;
    static InputStream modelIn2 = null;
    static InputStream modelIn3 = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        try {
            modelIn = hdfs.open(new Path(path + "en-ner-organization.bin"));
            System.out.println(modelIn.toString());
            modelIn2 = hdfs.open(new Path(path + "en-token.bin"));
            System.out.println(modelIn2.toString());
            modelIn3 = hdfs.open(new Path(path + "en-sent.bin"));
            System.out.println(modelIn3.toString());
        } catch (IllegalArgumentException | IOException ex) {
            Logger.getLogger(ImpactReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
        super.setup(context); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        for (Result value : values) {
            Put p = this.resultToPut(value);
            if (p != null) {
                context.write(null, p);
            }
        }
    }

    private Put resultToPut(Result result) {
        Put put = new Put(result.getRow());

        String words = HbaseHelper.createStringFromRawHbase(result, "content", "text");
        TextInspectorCompany company = new TextInspectorCompany();
        company.modelIn = modelIn;
        company.modelIn2 = modelIn2;
        company.modelIn3 = modelIn3;
        String[] foundCompanies = company.Inspect(words); // need /
        TextInspectorNumbers numbers = new TextInspectorNumbers(words);
        TextInspectorNumbersAndUnit numbersUnit = new TextInspectorNumbersAndUnit(words);
        if (numbers.foundWord != null || numbersUnit.foundWord != null) {
            put.add(Bytes.toBytes("impact"), Bytes.toBytes("number"), hbasehelper.HbaseHelper.getPutBytesSafe(numbers.foundWord));
            put.add(Bytes.toBytes("impact"), Bytes.toBytes("number_unit"), hbasehelper.HbaseHelper.getPutBytesSafe(numbersUnit.foundWord + ", " + numbersUnit.getUnit()));
            if (foundCompanies != null) {
                if (foundCompanies.length == 1) {
                    put.add(Bytes.toBytes("impact"), Bytes.toBytes("company"), hbasehelper.HbaseHelper.getPutBytesSafe(foundCompanies[0]));
                } else {
                    put.add(Bytes.toBytes("impact"), Bytes.toBytes("companies"), hbasehelper.HbaseHelper.getPutBytesSafe(Arrays.toString(foundCompanies)));
                }
            }
            return put;
        } else {
            return null;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (modelIn != null) {
            try {
                modelIn.close();

            } catch (IOException e) {
                Logger.getLogger(ImpactReducer.class.getName()).log(Level.SEVERE, "model 1", e);
            }
        }
        if (modelIn2 != null) {
            try {
                modelIn2.close();
            } catch (IOException e) {
                Logger.getLogger(ImpactReducer.class.getName()).log(Level.SEVERE, "model 2", e);
            }
        }
        if (modelIn3 != null) {
            try {
                modelIn3.close();
            } catch (IOException e) {
                Logger.getLogger(ImpactReducer.class.getName()).log(Level.SEVERE, "model 3", e);
            }
        }
        super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
    }

}
