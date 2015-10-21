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
public class TextInspectorNumbersAndUnit extends TextInspector {

    private ArrayList<String> units = new ArrayList<String>();
    private String unit;

    public TextInspectorNumbersAndUnit(String text) {
        units.add("hundred");
        units.add("hundreds");
        units.add("thousand");
        units.add("thousands");
        units.add("millions");
        units.add("million");
        units.add("milliard");
        units.add("milliards");
        units.add("billion");
        units.add("billions");
        units.add("users");
        units.add("accounts");
        units.add("people");
        String[] words = explodeText(text);
        for (String word : words) {
            if(Inspect(word)){
                break;
            }
        }
    }

    @Override
    protected boolean Inspect(String word) {
        if(foundWord != null)
        {
            inspectWordLikeUnit(word);
            if(unit != null)
            {
                return true;
            }
        }
        else{
            inspectNumbersOnly(word);
        }
        return false;
    }

    private boolean inspectWordLikeUnit(String word) {
        if (this.isValid(word)) {
            for (String s : this.units) {
                if (word.equalsIgnoreCase(s)) {
                    unit = word;
                }
            }
        }
        return false;
    }

    private boolean inspectNumbersOnly(String word) {
        if (this.isValid(word)) {
            char[] chars = new char[word.length()];
            word.getChars(0, word.length(), chars, 0);
            int countDigits = 0;
            for (char c : chars) {
                if (Character.isDigit(c)) {
                    countDigits++;
                }
            }
            if (countDigits >= 1) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

}
