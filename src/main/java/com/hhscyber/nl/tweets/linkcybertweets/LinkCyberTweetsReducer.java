/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.linkcybertweets;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

/**
 *
 * @author dev
 */
public class LinkCyberTweetsReducer extends TableReducer<Text, Result, Put> {

    public Result previousTweetRow = null;
    public Result currentTweetRow = null;
    public Result nextTweetRow = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.currentTweetRow = context.getCurrentValue();
    }

    @Override
    protected void reduce(Text key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        for (Result result : values) {
            if (result.getRow() == this.currentTweetRow.getRow()) {
                continue;
            }
            if (this.previousTweetRow != null && this.currentTweetRow == null) {
                this.currentTweetRow = result;
            } else if (this.currentTweetRow != null && this.nextTweetRow == null) {
                this.nextTweetRow = result;
            }
            LinkObject link = this.compareKeyAndCategory(this.previousTweetRow, this.currentTweetRow, this.nextTweetRow);
            this.writeRow(link,context);
            this.currentTweetRow = null;
        }
    }

    private LinkObject compareKeyAndCategory(Result previous, Result current, Result next) {
        String categoryPrevious = hbasehelper.HbaseHelper.createStringFromRawHbase(previous, "content", "category");
        String categoryCurrent = hbasehelper.HbaseHelper.createStringFromRawHbase(current, "content", "category");
        String categoryNext = hbasehelper.HbaseHelper.createStringFromRawHbase(next, "content", "category");

        String keywordPrevious = hbasehelper.HbaseHelper.createStringFromRawHbase(previous, "content", "keyword");
        String keywordCurrent = hbasehelper.HbaseHelper.createStringFromRawHbase(current, "content", "keyword");
        String keywordNext = hbasehelper.HbaseHelper.createStringFromRawHbase(next, "content", "keyword");
        if (categoryPrevious.equals(categoryCurrent) && keywordPrevious.equals(keywordCurrent) && categoryCurrent.equals(categoryNext) && keywordCurrent.equals(keywordNext)) {
            LinkObject tmp = new LinkObject(previous.getRow(), current.getRow(), next.getRow());
            tmp.setCurrentResult(current);
            return tmp;
        } else if (categoryCurrent.equals(categoryNext) && keywordCurrent.equals(keywordNext)) {
             LinkObject tmp = new LinkObject(current.getRow(), next.getRow());
             tmp.setCurrentResult(current);
             return tmp;
        }
        LinkObject tmp = new LinkObject(current.getRow());
        tmp.setCurrentResult(current);
        return tmp;
    }

    private void writeRow(LinkObject link, Context context) {
        Put put = new Put(link.currentRowKey);
        for (Cell c : link.getCurrentResult().rawCells()) {
            try {
                put.add(c);
            } catch (IOException ex) {
                Logger.getLogger(LinkCyberTweetsReducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        if(link.nextRowKey !=  null)
        {
            put.add(Bytes.toBytes("profile"), Bytes.toBytes("next_cyber_tweet"),link.nextRowKey);
        }
        if(link.previousRowKey != null)
        {
            put.add(Bytes.toBytes("profile"), Bytes.toBytes("next_cyber_tweet"),link.previousRowKey);
        }

        try {
            context.write(null, put);
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(LinkCyberTweetsReducer.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
