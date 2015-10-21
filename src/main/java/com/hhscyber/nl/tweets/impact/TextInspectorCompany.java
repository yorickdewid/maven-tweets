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

    protected boolean Inspect(String word) {
        char ch = word.charAt(0); // firs letter
        int count = 0;
        int countCaps = 0;
        if (Character.isUpperCase(ch)) {
            count++;
        }
        char[] chars = new char[word.length()];
        word.getChars(1, word.length(), chars, 0);
        for (char character : chars) {
            if (Character.isUpperCase(character)) {
                countCaps++;
            }
        }

        if (count == 1 && countCaps > 0 && countCaps <= 2) {
            return true; //likely to be company name
        } else {
            return false;
        }
    }

}
