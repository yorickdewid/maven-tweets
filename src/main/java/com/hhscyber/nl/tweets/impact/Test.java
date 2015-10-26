/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.impact;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.NameSample;
import opennlp.tools.namefind.NameSampleDataStream;
import opennlp.tools.namefind.TokenNameFinderEvaluator;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import opennlp.tools.util.Span;
import opennlp.tools.util.eval.FMeasure;

/**
 *
 * @author dev
 */
public class Test {

    public void testRead() {
        InputStream modelIn = null;
        try {
            modelIn = new FileInputStream("/home/dev/en-ner-organization.bin");
            try {
                TokenNameFinderModel model = new TokenNameFinderModel(modelIn);

                InputStream kaas = new FileInputStream("/home/dev/test.txt");
                InputStreamReader read = new InputStreamReader(kaas);
                ObjectStream<String> lineStream = new PlainTextByLineStream(read);

                ObjectStream<NameSample> sampleStream = new NameSampleDataStream(lineStream);
                TokenNameFinderEvaluator evaluator = new TokenNameFinderEvaluator(new NameFinderME(model));
                evaluator.evaluate(sampleStream);

                FMeasure result = evaluator.getFMeasure();

                System.out.println(result.toString());

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (modelIn != null) {
                    try {
                        modelIn.close();
                    } catch (IOException e) {
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                modelIn.close();
            } catch (IOException ex) {
                Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public void attemptTwo() throws IOException {
        InputStream modelIn = null;
        try {
            Tokenizer tokenizer = null;
            SentenceDetectorME sentenceDetector = null;
            modelIn = new FileInputStream("/home/dev/en-ner-organization.bin");
            try {
                TokenNameFinderModel model = new TokenNameFinderModel(modelIn);
                InputStream modelIn2 = new FileInputStream("/home/dev/en-token.bin");

                try {
                    TokenizerModel model2 = new TokenizerModel(modelIn2);

                    tokenizer = new TokenizerME(model2);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (modelIn2 != null) {
                        try {
                            modelIn2.close();
                        } catch (IOException e) {
                            throw e;
                        }
                    }
                }

                InputStream modelIn3 = new FileInputStream("/home/dev/en-sent.bin");

                try {
                    SentenceModel model3 = new SentenceModel(modelIn3);
                    sentenceDetector = new SentenceDetectorME(model3);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (modelIn3 != null) {
                        try {
                            modelIn3.close();
                        } catch (IOException e) {
                            throw e;
                        }
                    }
                }

                // somehow get the contents from the txt file
//      and populate a string called documentStr
                NameFinderME nameFinder = new NameFinderME(model);
                BufferedReader br = new BufferedReader(new FileReader("/home/dev/test.txt"));
                String line;
                while ((line = br.readLine()) != null) {
                    String sentences[] = sentenceDetector.sentDetect(line);
                    for (String sentence : sentences) {
                        String tokens[] = tokenizer.tokenize(sentence);
                        Span nameSpans[] = nameFinder.find(tokens);
                        // do something with the names
                        String[] arFound = Span.spansToStrings(nameSpans, tokens);
                        int count = 0;
                        for(String s : arFound)
                        {
                            count++;
                            System.out.println("Found " + count + " " + s);
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (modelIn != null) {
                    try {
                        modelIn.close();
                    } catch (IOException e) {
                        throw e;
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                modelIn.close();
            } catch (IOException ex) {
                Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    public static void main(String[] args) throws IOException {
        //new opennlp().testRead();
        new Test().attemptTwo();
    }
}
