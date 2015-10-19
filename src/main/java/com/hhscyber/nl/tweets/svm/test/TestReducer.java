/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.svm.test;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.tartarus.snowball.ext.englishStemmer;

/**
 *
 * @author eve
 */
public class TestReducer extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {

    private HConnection connection;
    FileSystem hdfs;
    private HTableInterface table;
    private Map words;
    SVMPrediect svm;
    englishStemmer stemmer;

    public TestReducer() {
        this.stemmer = new englishStemmer();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        connection = HConnectionManager.createConnection(context.getConfiguration());
        hdfs = FileSystem.get(context.getConfiguration());
        table = connection.getTable("hhscyber:svm_featureset");

        words = new TreeMap();
        InputStream is = hdfs.open(new Path("trainer.model"));
        BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        svm = new SVMPrediect(br);

        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            for (KeyValue kv : r.raw()) {
                words.put(new String(r.getValue(Bytes.toBytes("word"), Bytes.toBytes("index"))), new String(kv.getRow()));
            }
        }
    }

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put put : values) {
            List<Cell> cell = put.get(Bytes.toBytes("content"), Bytes.toBytes("text"));
            String data = Bytes.toString(CellUtil.cloneValue(cell.get(0)));

            if (isSpam(data)) {
                context.write(new ImmutableBytesWritable(Bytes.toBytes("hhscyber:tweets_spam")), put);
            } else {
                context.write(new ImmutableBytesWritable(Bytes.toBytes("hhscyber:tweets_filtered")), put);
            }
        }

    }

    private boolean isSpam(String data) throws IOException {
        System.out.println(data + " -> " + createSVMLine(data));
        String svmLine = createSVMLine(data);
        svm.setCurrent(svmLine);
        svm.predict();

        return true;
    }

    private String createSVMLine(String data) {
        String line = "-1";
        Set<Integer> hsline = new HashSet<>();
        StringTokenizer itr = new StringTokenizer(data);
        while (itr.hasMoreTokens()) {
            String tok = itr.nextToken().toLowerCase();
            stemmer.setCurrent(tok);
            stemmer.stem();
            String idx = (String) words.get(stemmer.getCurrent());
            if (idx != null) {
                hsline.add(Integer.parseInt(idx));
            }
        }

        List<Integer> asline = new ArrayList<>();
        asline.addAll(hsline);

        Collections.sort(asline);

        for (Integer index : asline) {
            line += " " + index + ":1";
        }

        return line;
    }
}
