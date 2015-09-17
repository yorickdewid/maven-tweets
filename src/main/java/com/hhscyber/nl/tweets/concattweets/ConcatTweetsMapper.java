package com.hhscyber.nl.tweets.concattweets;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author eve
 */
public class ConcatTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {

    // Alles wat je naar setup kunt verplaatsen....
    Text timestamp;

    public void setup(Context context) {
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        timestamp = new Text(this.getTimestampFromPath(filePathString));
        System.out.println("Timestamp " + timestamp);
    }

    @Override
    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
        context.write(timestamp, val);
    }

    public String getTimestampFromPath(String path) {
        String[] tmp = path.split("/");
        if (tmp[5] != null) {
            return tmp[6];
        } else {
            return null;
        }
    }
}
