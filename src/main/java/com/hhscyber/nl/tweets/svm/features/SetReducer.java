/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.features;

import io.github.htools.words.SnowballStemmer;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.tartarus.snowball.ext.englishStemmer;

/**
 *
 * @author eve
 */
public class SetReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    static int counter = 0;
    String lang = "english";
    englishStemmer stemmer;

    public SetReducer() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.stemmer = new englishStemmer();
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println(key.toString() + " Counter " + (counter + 1));
        String idx = Integer.toString(++counter);

        if (isValid(key.toString())) {

            String skey = key.toString().toLowerCase();
            stemmer.setCurrent(skey);
            stemmer.stem();

            Put put = new Put(Bytes.toBytes(idx));
            put.add(Bytes.toBytes("word"), Bytes.toBytes("index"), Bytes.toBytes(stemmer.getCurrent()));

            context.write(null, put);
        }
    }

    private boolean isValid(String s) {
        if (s.length() < 2) {
            return false;
        }
        if (s.contains("http://") || s.contains("https://")) {
            return false;
        }
        if (!s.matches("[#._@a-zA-Z0-9]{2,}")) {
            return false;
        }
        return true;
    }
}
