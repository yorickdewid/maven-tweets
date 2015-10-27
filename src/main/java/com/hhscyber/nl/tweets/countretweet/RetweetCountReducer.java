package com.hhscyber.nl.tweets.countretweet;

import hbasehelper.HbaseHelper;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author dev
 */
public class RetweetCountReducer extends TableReducer<ImmutableBytesWritable, Result, Put> {
    private static int countRetweets;
    private static int countTweets;

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        for (Result result : values) {
             String text = HbaseHelper.createStringFromRawHbase(result, "content", "text");
             String[] words = text.split(" ");
             if(Inspect(words[0])){
                 countRetweets++;
             }
             countTweets++;
        }
    }

    private boolean Inspect(String word) {
        char[] chars = new char[word.length()];
        word.getChars(0, word.length(), chars, 0);
        int count = 0;
        if (word.length() == 2) {
            if (chars[0] == 'R') {
                count++;
            }
            if(chars[1] == 'T') {
                count++;
            }
        }
        if(count == 2)
        {
            return true;
        }
        else{
            return false;
        }
    }

    @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Amount of Tweets " + countTweets);
        System.out.println("Amount of Retweets " + countRetweets);
        super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
    }

}
