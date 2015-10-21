/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.impact;

/**
 *
 * @author dev
 */
public abstract class TextInspector {
    
    protected String foundWord;
 
    protected String[] explodeText(String text) {
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
        if (s.contains("@")) {
            return false;
        }
        if (s.matches("[#._@a-zA-Z0-9]{2,}")) {
            return false;
        }
        return true;
    }
   
}
