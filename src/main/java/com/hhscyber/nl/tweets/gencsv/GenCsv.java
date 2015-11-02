/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.gencsv;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.DirComponent;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 *
 * @author eve
 */
public class GenCsv {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args, "");
        FileSystem hdfs = FileSystem.get(conf);
        conf.set("outputpath", "location");
        Job job = new Job(conf, "GenerateCsv");
        job.setJarByClass(GenCsv.class);
        String stop = "634628247817359360"; //1000 tweets? add 1 row
        Scan scan = new Scan();
        //scan.setStopRow(stop.getBytes());
        job.setSpeculativeExecution(false);
        FilterList filterList = new FilterList();
        SingleColumnValueFilter filterCity = new SingleColumnValueFilter(hbasehelper.HbaseHelper.getPutBytesSafe("content"),
                hbasehelper.HbaseHelper.getPutBytesSafe("location_city"),
                CompareFilter.CompareOp.NOT_EQUAL,
                hbasehelper.HbaseHelper.getPutBytesSafe(""));
        SingleColumnValueFilter filterKnown = new SingleColumnValueFilter(hbasehelper.HbaseHelper.getPutBytesSafe("content"),
                hbasehelper.HbaseHelper.getPutBytesSafe("location_known"),
                CompareFilter.CompareOp.EQUAL,
                hbasehelper.HbaseHelper.getPutBytesSafe("true"));
        filterList.addFilter(filterCity);
        filterList.addFilter(filterKnown);
        scan.setFilter(filterList);

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets_final", scan, GenCsvMapper.class, ImmutableBytesWritable.class, Result.class, job);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setReducerClass(GenCsvReducer.class);

        hdfs.delete(new Path("location"), true);

        job.waitForCompletion(true);
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
