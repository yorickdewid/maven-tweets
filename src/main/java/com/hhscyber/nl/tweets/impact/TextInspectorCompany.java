package com.hhscyber.nl.tweets.impact;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author dev
 */
public class TextInspectorCompany {

    public SentenceModel sentenceModel = null;
    public TokenizerModel tokenizerModel = null;
    public TokenNameFinderModel modelToken = null;

    public String[] Inspect(String text) {
        NameFinderME nameFinder = new NameFinderME(modelToken);
        Tokenizer tokenizer = new TokenizerME(tokenizerModel);
        SentenceDetectorME sentenceDetector = new SentenceDetectorME(sentenceModel);
        String sentences[] = sentenceDetector.sentDetect(text);
        ArrayList<String[]> tmp = new ArrayList<>();
        for (String sentence : sentences) {
            String tokens[] = tokenizer.tokenize(sentence);
            Span nameSpans[] = nameFinder.find(tokens);
            String[] arFound = Span.spansToStrings(nameSpans, tokens);
            tmp.add(arFound);
        }
        String[] arFound = null;
        for (String[] s : tmp) {
            if (arFound == null) {
                arFound = s;
            } else {
                arFound = concat(arFound, s);
            }
        }
        return arFound;
    }

    public String[] concat(String[] a, String[] b) {
        int aLen = a.length;
        int bLen = b.length;
        String[] c = new String[aLen + bLen];
        System.arraycopy(a, 0, c, 0, aLen);
        System.arraycopy(b, 0, c, aLen, bLen);
        return c;
    }

}
