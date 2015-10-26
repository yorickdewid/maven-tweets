/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.impact;

import java.util.ArrayList;

/**
 *
 * @author dev
 */
public abstract class TextInspector {
    
    protected String foundWord;
 
    protected String[] explodeText(String text) {
        addNegativeSpamList();
        String[] explode = text.split(" ");
        return explode;
    }
   
    public String getFoundWord() {
        return foundWord;
    }
    
    protected abstract boolean Inspect(String word);
    
    protected boolean isValid(String s) {
        if (s.contains("http://") || s.contains("https://")) {
            return false;
        }
        if (s.contains("(") || s.contains(")")) {
            return false;
        }
        if (s.contains("#")) {
            return false;
        }
        if (s.contains("@")) {
            return false;
        }
        if (s.contains(":")) {
            return false;
        }
//        for(String negative : spamWordsNegative)
//        {
//            if(s.toLowerCase().contentEquals(negative))
//            {
//                return false;
//            }
//        }
        return true;
    }
    
    private static ArrayList<String> spamWordsNegative = new ArrayList<>();


    /**
     * EXTREMLY SLOW
     */
    private static void addNegativeSpamList() {
        spamWordsNegative.add("xxx");
        spamWordsNegative.add("porn");
        spamWordsNegative.add("pussy");
        spamWordsNegative.add("ass");
        spamWordsNegative.add("gay");
        spamWordsNegative.add("dick");
        spamWordsNegative.add("wedding");
        spamWordsNegative.add("bum");
        spamWordsNegative.add("relationship");
        spamWordsNegative.add("eyes");
        spamWordsNegative.add("wife");
        spamWordsNegative.add("fucked");
        spamWordsNegative.add("rain");
        spamWordsNegative.add("rainFall");
        spamWordsNegative.add("flooding");
        spamWordsNegative.add("drains");
        spamWordsNegative.add("shit");
        spamWordsNegative.add("die");
        spamWordsNegative.add("pitch");
        spamWordsNegative.add("football");
        spamWordsNegative.add("twat");
        spamWordsNegative.add("nfsw");
        spamWordsNegative.add("knife");
        spamWordsNegative.add("fucking");
        spamWordsNegative.add("traded");
        spamWordsNegative.add("impressions");
        spamWordsNegative.add("immigrants");
        spamWordsNegative.add("immigrant");
    }
}
