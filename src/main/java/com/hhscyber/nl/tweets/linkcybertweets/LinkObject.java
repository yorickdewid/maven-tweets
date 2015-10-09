/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.linkcybertweets;

import org.apache.hadoop.hbase.client.Result;

/**
 *
 * @author dev
 */
public class LinkObject {
    
    public byte[] previousRowKey;
    public byte[] currentRowKey;
    public byte[] nextRowKey;
    
    private Result previousResult = null;
    private Result currentResult = null;
    private Result nextResult = null;

    public LinkObject(byte[] previousRowKey, byte[] currentRowKey, byte[] nextRowKey) {
        this.previousRowKey = previousRowKey;
        this.currentRowKey = currentRowKey;
        this.nextRowKey = nextRowKey;
    }
    
    public LinkObject(byte[] currentRowKey, byte[] nextRowKey) {
        this.currentRowKey = currentRowKey;
        this.nextRowKey = nextRowKey;
    }
    
     public LinkObject(byte[] currentRowKey) {
        this.currentRowKey = currentRowKey;
    }

    public void setCurrentResult(Result currentResult) {
        this.currentResult = currentResult;
    }

    public void setNextResult(Result nextResult) {
        this.nextResult = nextResult;
    }

    public void setPreviousResult(Result previousResult) {
        this.previousResult = previousResult;
    }

    public Result getCurrentResult() {
        return currentResult;
    }

    public Result getNextResult() {
        return nextResult;
    }

    public Result getPreviousResult() {
        return previousResult;
    }
}
