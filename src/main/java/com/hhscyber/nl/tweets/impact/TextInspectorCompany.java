package com.hhscyber.nl.tweets.impact;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author dev
 */
public class TextInspectorCompany extends TextInspector {

    public TextInspectorCompany(String text) {
        String[] words = explodeText(text);
        for (String word : words) {
            if (Inspect(word)) {
                this.foundWord = word;
            }
        }
    }

    @Override
    protected boolean Inspect(String word) {
        if (word.length() > 0) {
            char ch = word.charAt(0); // first letter
            int count = 0;
            int countCaps = 0;
            if (Character.isUpperCase(ch)) {
                count++;
            }
            char[] chars = new char[word.length()];
            word.getChars(1, word.length(), chars, 0);
            for (int i = 0; i < word.length(); i++) {
                if (Character.isUpperCase(chars[i])) {
                    if(i == 0)
                    {
                        countCaps++;
                    }
                    else if((i % 2) != 0){
                        countCaps++;
                    }
                }
            }
            if (count == 1 && countCaps > 0 && countCaps <= 2) {
                return true; //likely to be company name
            } else {
                return false;
            }
        }
        return false;
    }

}
