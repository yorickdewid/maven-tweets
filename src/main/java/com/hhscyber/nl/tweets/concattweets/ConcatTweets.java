package com.hhscyber.nl.tweets.concattweets;

import io.github.htools.io.DirComponent;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * @author eve
 */
public class ConcatTweets {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        // valkuil: in Job constructor wordt conf gecopieerd, dus alles wat je in conf wilt
        // zetten moet je doen voordat je de job aanmaakt, of in de conf van job.
        conf.set("outputpath", "jsonconcat");

        Job client = new Job(conf);
        client.setSpeculativeExecution(false);
        client.setJarByClass(ConcatTweets.class);
        client.setJobName(ConcatTweets.class.getCanonicalName());

        // vergeten mapOutput types te zetten
        client.setMapOutputKeyClass(Text.class);
        client.setMapOutputValueClass(Text.class);

        // geen outputformat (zie verderop) dus ook geen types instellen
//        client.setOutputKeyClass(Text.class);
//        client.setOutputValueClass(Text.class);
        client.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path("input/143*");

        TextInputFormat.addInputPath(client, inputPath); //1440* ...
        // zet aantal reducers naar aantal timestamps
        client.setNumReduceTasks(countReducers(conf, inputPath));

        // geen textoutputformat als je direct naar file schrijft!
        // TextOutputFormat.setOutputPath(client, new Path("jsonconcat"));
        client.setOutputFormatClass(NullOutputFormat.class);
        // schrijf eventuele parameters in je configuration

        client.setMapperClass(ConcatTweetsMapper.class);
        client.setReducerClass(ConcatTweetsReducer.class);

        hdfs.delete(new Path("jsonconcat"), true);

        // over het algemeen krijg je toch wel een stacktrace als iets mis gaat
        // dus je kunt throwen ipv afvangen
        try {
            client.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // telt het aantal timestamps/foldernames
    public static int countReducers(FileSystem fs, Path inputPath) throws IOException {
        HashSet<String> timestamps = new HashSet();
        // native Hadoop, maar werkt niet met * wildcards
        for (FileStatus fileStatus : fs.listStatus(inputPath)) {
            String timestamp = fileStatus.getPath().getName();
            timestamps.add(timestamp);
        }
        return timestamps.size();
    }
    
    public static int countReducers(Configuration conf, Path inputPath) throws IOException {
        HashSet<String> timestamps = new HashSet();
        HDFSPath inHdfsPath = new HDFSPath(conf, inputPath);
        for (DirComponent path : inHdfsPath.wildcardIterator()) {
            timestamps.add(path.getName());
        }
        return timestamps.size();
    }
}
