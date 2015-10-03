/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.customspamfilter;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
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
    }

    private static void addPositiveSpamList(Result result) {
        byte[] b = getValueSafe(result, "content", "keyword");
        String keyword = createStringFromByte(b);

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
        byte[] b = getValueSafe(result, "content", "text");
        byte[] b2 = getValueSafe(result, "profile", "verified");
        byte[] b3 = getValueSafe(result, "content", "retweet_count");
        byte[] b4 = getValueSafe(result, "profile", "followers_count");
        byte[] b5 = getValueSafe(result, "content", "favorite_count");

        int countSpam = 0;

        String text = createStringFromByte(b);
        String verified = createStringFromByte(b2);

        int retweetCount = createIntegerFromByte(b3);
        int followersCount = createIntegerFromByte(b4);
        int favoriteCount = createIntegerFromByte(b5);
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

        String row = createStringFromByte(result.getRow());
        countSpam += filterText(text, countSpam);
        countSpam += checkNumberAffected(text, countSpam);

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

    private static int checkNumberAffected(String text, int countSpam) {
        int countPoints = countSpam;
        int[] numbers = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        for (int n : numbers) {
            if (text.contains(Integer.toString(n))) {
                countPoints += 1;
            }
            if (text.contains("millions") || text.contains("thousands") || text.contains("hundreds")) {
                countPoints += 2;
            }
        }
        return countPoints;
    }

    /**
     * Stop nullpointers
     *
     * @param b
     * @return
     */
    private static String createStringFromByte(byte[] b) {
        if (b != null) {
            return new String(b);
        } else {
            return "";
        }
    }

    private static int createIntegerFromByte(byte[] b) {
        if (b != null) {
            int count = new BigInteger(b).intValue();
            return count;
        } else {
            return 0;
        }
    }

    private static byte[] getValueSafe(Result result, String family, String column) {
        if (result.containsColumn(Bytes.toBytes(family), Bytes.toBytes(column))) {
            return result.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
        } else {
            return null;
        }
    }

}
