/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.url;

import java.io.IOException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author dev
 */
public class test {

    static String[] urls = {"[http://bit.ly/1gp8oFw]", "[http://clean.mx/m?77373444]", "[http://bit.ly/1gp8oFw]"}; //works

    public static void main(String[] args) {
        String html = "<HTML><HEAD><TITLE>Moved Permanently</TITLE></HEAD><BODY BGCOLOR=\"#FFFFFF\" TEXT=\"#000000\"><H1>Moved Permanently</H1>The document has moved <A HREF=\"http://www.photoreflect.com/store/Orderpage.aspx?pi=1K4G004W000233\">here</A>.</BODY></HTML>";
        makeModular(html);
    }

    private static String kurwaFunction() {
        try {
            //Url URL = new Url();
            for (String url : urls) {
                url = cleanUrl(url); //always clean first
                if (url.contains("t.co") || url.contains("bit.ly")) {
                    URLUnshortener shortener = new URLUnshortener();
                    URL url2 = shortener.expand(new URL(url));
                    url = url2.toString();
                }
                Document doc = Jsoup.connect(url).get();
                Elements els = doc.getAllElements();
                if (els.hasAttr("title") || els.hasAttr("h1")) {
                    for (Element el : els) {
                        boolean href = true;
                        if (el.nodeName().equals("a") && href) {
                            if (el.text().toLowerCase().equals("here")) {
                                Element el1 = el.attr("href", "");
                                System.out.println(el1);
                            }
                        }
                        switch (el.nodeName()) {
                            case "title":
                                System.out.println(el.text());
                                if (!el.text().isEmpty()) {
                                    break;
                                }
                            case "h1":
                                if (el.text().contains("moved") && el.text().contains("has")) {
                                    href = true;
                                }
                                break;
                            default:
                                break;
                        }
                    }
                }
            }

        } catch (HttpStatusException ex) {
            System.out.println("Exception http " + ex.getStatusCode());
        } catch (IOException ex) {
            Logger.getLogger(test.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "";
    }

    private static String makeModular(String html) {
        String text = "";
        Document doc = Jsoup.parse(html);
        Elements els = doc.getAllElements();
        boolean moved = false;
        String url = "";
        for (Element el : els) {
            switch (el.nodeName()) {
                case "title":
                    text = el.text();
                    if(text.toLowerCase().contains("moved") && text.toLowerCase().contains("permanently"))
                    {
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
        if(moved)
        {
            getMovedUrl(doc);
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
        url.getChars(index[0] + 1, index[1], chars, 0);
        url = new String(chars, 0, chars.length);
        return url;
    }
}
