/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.locationorig;

import com.hhscyber.nl.tweets.impact.*;
import hbasehelper.HbaseHelper;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerModel;
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
public class LocationOriginReducer extends TableReducer<ImmutableBytesWritable, Result, Put> {

    String path = "nlp_models/";
    /*
     NOTE MODEL CAN BE INSTANTIATED ONLY ONCE IN 1 THREAD
     */
    static TokenizerModel tokenizerModel = null;
    static SentenceModel sentenceModel = null;
    static TokenNameFinderModel tokenModel = null;
    static int count = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        count++;
        System.out.println("Setup called, count = " + count);
        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        try {
            InputStream model1 = hdfs.open(new Path(path + "en-ner-location.bin"));
            tokenModel = new TokenNameFinderModel(model1);
            model1.close();
            InputStream model2 = hdfs.open(new Path(path + "en-token.bin"));
            tokenizerModel = new TokenizerModel(model2);
            model2.close();
            InputStream model3 = hdfs.open(new Path(path + "en-sent.bin"));
            sentenceModel = new SentenceModel(model3);
            model3.close();
        } catch (IllegalArgumentException | IOException ex) {
            Logger.getLogger(LocationOriginReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
        super.setup(context); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        for (Result value : values) {
            Put p = this.resultToPut(value);
            if (p != null) {
                context.write(null, p);
                count++;
            }
        }
    }

    private Put resultToPut(Result result) {
        Put put = new Put(result.getRow());

        String words = HbaseHelper.createStringFromRawHbase(result, "content", "text");
        TextInspectorLocation locInspector = new TextInspectorLocation();
        locInspector.modelToken = tokenModel;
        locInspector.tokenizerModel = tokenizerModel;
        locInspector.sentenceModel = sentenceModel;
        String[] locations = locInspector.Inspect(words);
        if (locations != null) {
            if (locations.length == 1) {
                put.add(Bytes.toBytes("location"), Bytes.toBytes("org_location"), hbasehelper.HbaseHelper.getPutBytesSafe(locations[0]));
            } else {
                put.add(Bytes.toBytes("location"), Bytes.toBytes("org_locations"), hbasehelper.HbaseHelper.getPutBytesSafe(Arrays.toString(locations)));
            }
        }
        return put;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Processed "+count+" tweets");
        super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
    }

}
