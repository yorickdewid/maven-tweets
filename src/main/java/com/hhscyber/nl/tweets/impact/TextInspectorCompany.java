package com.hhscyber.nl.tweets.impact;

import java.io.FileInputStream;
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

    public InputStream modelIn = null;
    public InputStream modelIn2 = null;
    public InputStream modelIn3 = null;

    public String[] Inspect(String text) {
        try {

            TokenNameFinderModel model = new TokenNameFinderModel(modelIn);
            NameFinderME nameFinder = new NameFinderME(model);

            TokenizerModel model2 = new TokenizerModel(modelIn2);
            Tokenizer tokenizer = new TokenizerME(model2);

            SentenceModel model3 = new SentenceModel(modelIn3);
            SentenceDetectorME sentenceDetector = new SentenceDetectorME(model3);

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
        } catch (IOException ex) {
            Logger.getLogger(TextInspectorCompany.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
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
