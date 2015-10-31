/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.select;

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
public class SelectTweets {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        Conf conf = new Conf(args, "");
        Job job = new Job(conf, "SelectTweets");
        job.setJarByClass(SelectTweets.class);
        Scan scan = new Scan();
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
        SingleColumnValueFilter filterImpactNumber = new SingleColumnValueFilter(hbasehelper.HbaseHelper.getPutBytesSafe("content"),
                hbasehelper.HbaseHelper.getPutBytesSafe("impact_number"),
                CompareFilter.CompareOp.NOT_EQUAL,
                hbasehelper.HbaseHelper.getPutBytesSafe(""));
        SingleColumnValueFilter filterImpactOrganization = new SingleColumnValueFilter(hbasehelper.HbaseHelper.getPutBytesSafe("content"),
                hbasehelper.HbaseHelper.getPutBytesSafe("impact_company"),
                CompareFilter.CompareOp.NOT_EQUAL,
                hbasehelper.HbaseHelper.getPutBytesSafe(""));
        filterList.addFilter(filterCity);
        filterList.addFilter(filterKnown);
        filterList.addFilter(filterImpactNumber);
        filterList.addFilter(filterImpactOrganization);
        scan.setFilter(filterList);

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets_final", scan, SelectTweetsMapper.class, ImmutableBytesWritable.class, Result.class, job);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setReducerClass(SelectTweetsReducer.class);

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
