package com.hhscyber.nl.tweets.concattweets;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author eve
 */
public class ConcatTweetsReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem hdfs = FileSystem.get(conf);
        StringBuilder sb = new StringBuilder();
        Path baseOutputPath = new Path(conf.get("outputpath"));
        Path newFilePath = Path.mergePaths(baseOutputPath, new Path("/" + key + ".json"));
        try {
            for (Text value : values) {
                sb.append(value.toString());
                sb.append("\n");
            }
            byte[] byt = sb.toString().getBytes();

            try (FSDataOutputStream fsOutStream = hdfs.create(newFilePath)) {
                fsOutStream.write(byt);
            }
            // nooit zomaar vage exceptions afvangen, bovenstaande gooit nooit een
            // exception, en bij outofmemory krijg je toch wel een stacktrace
        } catch (Exception ex) {
            Logger.getLogger(ConcatTweetsReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
