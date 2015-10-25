/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
    package com.hhscyber.nl.tweets.url;

import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

/**
 *
 * @author eve
 */
public class CrawlUrl {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {
        String outputTableName = "hhscyber:tweets_titles_test";
        Conf conf = new Conf(args,"");
        Job job = new Job(conf, "CrawlUrl");
        job.setJarByClass(CrawlUrl.class);
        String stop = "633218488010735617"; //475 tweets
        Scan scan = new Scan();
        //scan.setStopRow(stop.getBytes());
                //List<Filter> rowFilters = new ArrayList<>();
                SingleColumnValueFilter filterColumn = new SingleColumnValueFilter(hbasehelper.HbaseHelper.getPutBytesSafe("content"),
                hbasehelper.HbaseHelper.getPutBytesSafe("urls"),
                CompareFilter.CompareOp.NOT_EQUAL,
                hbasehelper.HbaseHelper.getPutBytesSafe(""));
//        rowFilters.add(filterColumn);
//        
//        FilterList filters = new FilterList(rowFilters);
        scan.setFilter(filterColumn);

        TableMapReduceUtil.initTableMapperJob("hhscyber:tweets_lang", scan, CrawlUrlMapper.class, ImmutableBytesWritable.class, Result.class, job);
        job.setNumReduceTasks(10);
        
        TableMapReduceUtil.initTableReducerJob(outputTableName, CrawlUrlReducer.class, job); // if disabled no output folder specfied exception

        
        job.waitForCompletion(true);
    }
    
}
