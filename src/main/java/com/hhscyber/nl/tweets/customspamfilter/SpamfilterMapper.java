/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.customspamfilter;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eve
 */
public class SpamfilterMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private static ArrayList<String> spamWordsNegative = new ArrayList<>();
    private static ArrayList<String> spamWordsPositive = new ArrayList<>();

    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        addNegativeSpamList();
        addPositiveSpamList(value);
        filterSpam(row, value);
    }

    private static void addNegativeSpamList() {
        spamWordsNegative.add("xxx");
        spamWordsNegative.add("porn");
        spamWordsNegative.add("pussy");
        spamWordsNegative.add("gay");
        spamWordsNegative.add("dick");
        spamWordsNegative.add("wedding");
        spamWordsNegative.add("bum");
        spamWordsNegative.add("relationship");
        spamWordsNegative.add("eyes");
        spamWordsNegative.add("wife");
        spamWordsNegative.add("children");
    }

    private static void addPositiveSpamList(Result result) {
        byte[] b = hbasehelper.HbaseHelper.getValueSafe(result, "content", "keyword");
        String keyword = hbasehelper.HbaseHelper.createStringFromByte(b);

        /*
         EXPLANANTION
         keywords also match in profile of twitter user so might be irrelvant to the current tweet
         */
        spamWordsPositive.add("#" + keyword);
        spamWordsPositive.add(keyword);
        spamWordsPositive.add("discovered");
        spamWordsPositive.add("detected");
        spamWordsPositive.add("affected");
    }

    /**
     * @param key
     * @param result
     */
    private static void filterSpam(ImmutableBytesWritable key, Result result) {
        byte[] b = hbasehelper.HbaseHelper.getValueSafe(result, "content", "text");
        byte[] b2 = hbasehelper.HbaseHelper.getValueSafe(result, "profile", "verified");
        byte[] b3 = hbasehelper.HbaseHelper.getValueSafe(result, "content", "retweet_count");
        byte[] b4 = hbasehelper.HbaseHelper.getValueSafe(result, "profile", "followers_count");
        byte[] b5 = hbasehelper.HbaseHelper.getValueSafe(result, "content", "favorite_count");

        int countSpam = 0;

        String text = hbasehelper.HbaseHelper.createStringFromByte(b);
        String verified = hbasehelper.HbaseHelper.createStringFromByte(b2);

        int retweetCount = hbasehelper.HbaseHelper.createIntegerFromByte(b3);
        int followersCount = hbasehelper.HbaseHelper.createIntegerFromByte(b4);
        int favoriteCount = hbasehelper.HbaseHelper.createIntegerFromByte(b5);
        if (verified.equals("true")) {
            countSpam += 5;
        }
        //retweets
        if (retweetCount >= 10) {
            countSpam += 5;
        } else if (retweetCount < 0) {
            countSpam -= 5;
        }
        //followers
        if (followersCount >= 100) {
            countSpam += 5;
        } else if (followersCount < 40) {
            countSpam -= 5;
        }
        //favorites less valuable than retweets and followers
        if (favoriteCount >= 5) {
            countSpam += 5;
        }

        String row = hbasehelper.HbaseHelper.createStringFromByte(result.getRow());
        countSpam += filterText(text, countSpam);

        if (countSpam >= 10) {
            System.out.println(row + " Good");
        } else if (countSpam == 0 && countSpam < 10) {
            System.out.println(row + " Ignore");
        } else if (countSpam < 0) {
            System.out.println(row + " Wrong");
        }

    }

    private static int filterText(String text, int countSpam) {
        int countPoints = countSpam;
        for (String spamWord : spamWordsNegative) { //minus part
            if (countPoints == -100) {
                break;
            } else {
                if (text.contains(spamWord)) {
                    countPoints -= 1;
                }
            }
        }

        for (String spamWord : spamWordsPositive) { //plus part
            if (countPoints == 100) {
                break;
            } else {
                if (text.contains(spamWord)) {
                    countPoints += 1;
                }
            }
        }

        return countPoints;
    }

}
