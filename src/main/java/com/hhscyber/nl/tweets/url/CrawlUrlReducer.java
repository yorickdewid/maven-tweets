package com.hhscyber.nl.tweets.url;

import com.hhscyber.nl.tweets.linkcybertweets.LinkCyberTweetsReducer;
import com.hhscyber.nl.tweets.locationcount.*;
import java.io.IOException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import static hbasehelper.HbaseHelper.getPutBytesSafe;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author dev
 */
public class CrawlUrlReducer extends TableReducer<ImmutableBytesWritable, Result, Put> {

    public static countObject count = new countObject();

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
        for (Result result : values) {
            String url = getUrlFromUrls(result);
            if (!url.equals("")) {
                String build = getUrlContent(url);
                Put put = new Put(result.getRow());
                put.add(getPutBytesSafe("content"), getPutBytesSafe("title"), getPutBytesSafe(build));
                context.write(null, put);
            }
        }
    }

    private static String getUrlFromUrls(Result result) {
        return hbasehelper.HbaseHelper.createStringFromRawHbase(result, "content", "urls");
    }

    private static String getUrlContent(String url) {
        try {
            if (!url.equals("")) {
                url = cleanUrl(url); //always clean first
                if (url.contains("t.co") || url.contains("bit.ly")) {
                    URLUnshortener shortener = new URLUnshortener();
                    URL url2 = shortener.expand(new URL(url));
                    url = url2.toString();
                }
                Elements els = getElementsFromHtml(url);
                if (els != null) {
                    return extractTitle(els);
                } else {
                    return "";
                }
            }

        } catch (IOException ex) {
            Logger.getLogger(test.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "";
    }

    private static Elements getElementsFromHtml(String url) {
        try {
            Document doc = Jsoup.connect(url).get();
            Elements els = doc.getAllElements();
            return els;
        } catch (HttpStatusException ex) {
            System.out.println("Skip because Http error code: " + ex.getStatusCode());
        } catch (IOException ex) {
            Logger.getLogger(CrawlUrlReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private static String extractTitle(Elements els) {
        String text = "";
        boolean moved = false;
        String url = "";
        for (Element el : els) {
            switch (el.nodeName()) {
                case "title":
                    text = el.text();
                    if (text.toLowerCase().contains("moved") && text.toLowerCase().contains("permanently")) {
                        moved = true;
                    }
                    break;
                case "body":
                    if (moved) {
                        url = getMovedUrl(el);
                    }
                    break;
                default:
                    break;
            }
        }
        if (moved) {
            text = extractTitle(getElementsFromHtml(url));
        }
        return text;
    }

    private static String getMovedUrl(Element el) {
        String url = "";
        Elements body = el.getAllElements();
        for (Element bodyel : body) {
            if (bodyel.nodeName().equals("a")) {
                url = bodyel.absUrl("href");
            }
        }
        return url;

    }

    private static String cleanUrl(String url) {
        int[] index = new int[2];
        index[0] = url.indexOf("[");
        index[1] = url.indexOf("]");
        char[] chars = new char[index[1]];
        url.getChars(index[0] + 1, index[1] - 1, chars, 0);
        url = new String(chars, 0, chars.length);
        return url;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
    }

}
