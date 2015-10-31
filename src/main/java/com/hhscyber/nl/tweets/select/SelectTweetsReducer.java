package com.hhscyber.nl.tweets.select;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author eve
 */
public class SelectTweetsReducer extends Reducer<ImmutableBytesWritable, Result, NullWritable, NullWritable> {

    static int count = 0;
    static int countReliable = 0;
    static int countWithoutPercentage = 0;

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        for (Result value : values) {
            String text = hbasehelper.HbaseHelper.createStringFromRawHbase(value, "content", "text");
            String verified = hbasehelper.HbaseHelper.createStringFromRawHbase(value, "profile", "verified");
            if (verified.equals("true")) {
                countReliable++;
                if (!text.contains("%")) {
                    countWithoutPercentage++; //saves 10.000 tweets good enough
                    System.out.println("Text " + text);
                }
            }
//            byte[] b = hbasehelper.HbaseHelper.getValueSafe(value, "content", "favorite_count");
//            int favoriteCount = hbasehelper.HbaseHelper.createIntegerFromByte(b);
//            if (favoriteCount > 100) { // between 10 and 100 is a huge difference 100.000+ tweets result isnt satisfying
//                System.out.println("Text " + text);
//                countReliable++;
//            }

            count++;
//            String text = new String(hbasehelper.HbaseHelper.getValueSafe(value, "content", "text"));
//            System.out.println("Text " + text);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Amount of tweets " + count);
        System.out.println("Amount of non-percentage tweets " + countWithoutPercentage);
        System.out.println("Amount of reliable tweets " + countReliable);
    }

}
