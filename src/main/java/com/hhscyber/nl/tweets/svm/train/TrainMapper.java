/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.train;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author eve
 */
public class TrainMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text text = new Text();
    private HConnection connection;
    private HTableInterface table;
    private Map words;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        connection = HConnectionManager.createConnection(context.getConfiguration());
        table = connection.getTable("hhscyber:svm_featureset");

        words = new TreeMap();

        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            for (KeyValue kv : r.raw()) {
                words.put(new String(r.getValue(Bytes.toBytes("word"), Bytes.toBytes("index"))), new String(kv.getRow()));
            }

        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        String[] tokens = filePathString.split("\\.(?=[^\\.]+$)");
        System.out.println(filePathString);

        Text label;
        String line;
        if (tokens[1].equals("bad")) {
            label = new Text("bad");
            line = "1";
        } else {
            label = new Text("good");
            line = "-1";
        }

        Set<Integer> hsline = new HashSet<>();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String tok = itr.nextToken();
            String idx = (String) words.get(tok);
            if (idx != null) {
                System.out.println(tok + " IDX " + idx);
                hsline.add(Integer.parseInt(idx));
            }
        }

        List<Integer> asline = new ArrayList<>();
        asline.addAll(hsline);
        
        Collections.sort(asline);

        for (Integer index : asline) {
            line += " " + index + ":1";
        }
        context.write(label, new Text(line));
    }
}
