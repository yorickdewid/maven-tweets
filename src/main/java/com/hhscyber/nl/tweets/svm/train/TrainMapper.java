/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.train;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eve
 */
public class TrainMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text text = new Text();
    private HConnection connection;
    private HTableInterface table;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //Configuration conf = context.getConfiguration();
        //table = new HTable(conf, "hhscyber:svm_featureset");
        connection = HConnectionManager.createConnection(context.getConfiguration());

        //HConnection connection = HConnectionManager.createConnection(context.getConfiguration());
        table = connection.getTable("hhscyber:tweets_test");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            findWordIndex(itr.nextToken());
            System.out.println("value " + value);
            /*text.set(itr.nextToken());
             context.write(text, new IntWritable(1));*/
        }
        byte[] tableName = table.getTableName();
        System.out.println("tablename " + new String(tableName));
    }

    public String findWordIndex(String word) throws IOException {
        Scan scan = new Scan();
        //scan.addColumn(Bytes.toBytes("word"), Bytes.toBytes("index"));
        //scan.setRowPrefixFilter(Bytes.toBytes(word));

        /*SingleColumnValueFilter filter = new SingleColumnValueFilter(
         Bytes.toBytes("word"),
         Bytes.toBytes("index"),
         CompareOp.EQUAL,
         Bytes.toBytes(word)
         );
         scan.setFilter(filter);*/
        /* ResultScanner rs = table.getScanner(scan);
         for (Result r = rs.next(); r != null; r = rs.next()) {
         for (KeyValue kv : r.raw()) {
         System.out.println("Found key: " + new String(kv.getValueArray()));
         System.out.println("Found key: " + new String(kv.getKey()));
         }
         }*/
        return "";
    }
}
