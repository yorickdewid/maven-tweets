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
public class TextInspectorNumbers extends TextInspector {

    public TextInspectorNumbers(String text) {
        String[] words = explodeText(text);
        for (String word : words) {
            if(Inspect(word)){
                foundWord = word;
            }
        }
    }

    @Override
    protected boolean Inspect(String word) {
        if (this.isValid(word)) {
            char[] chars = new char[word.length()];
            word.getChars(0, word.length(), chars, 0);
            int countDigits = 0;
            int countDescriptor = 0;
            for (char c : chars) {
                countDescriptor = this.countDescriptor(c);
                if (Character.isDigit(c)) {
                    countDigits++;
                }
            }
            if (countDigits >= 1 && countDescriptor == 1) {
                return true;
            }
            else{
                return false;
            }
        }
        return false;
    }

    /**
     * Million Milliard Billion Thousands Hundreds
     *
     * @param c
     * @return
     */
    private int countDescriptor(char c) {
        int count = 0;
        if (Character.isLetter(c)) {
            if (c == 'k' || c == 'm') {
                count++;
            }
        }
        return count;
    }
}
