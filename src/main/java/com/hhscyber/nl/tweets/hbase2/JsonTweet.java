/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbase2;

import java.util.ArrayList;

/**
 *
 * @author eve
 */
public class JsonTweet {

    private final String id;
    private String text;
    private String retweet_count;
    private String favorite_count;
    private String contributors;
    private String lang;
    private String geo;
    private String source;
    private String created_at;
    private String place;
    private String retweeted;
    private String truncated;
    private String favorited;
    private String coordinates;
    private String keyword;
    private final JsonTweetUser profile;
    private ArrayList<String> urlList;

    public JsonTweet(String id, String pid) {
        this.id = id;
        this.profile = new JsonTweetUser(pid);
        urlList = new ArrayList<>();
    }

    public void addUrl(String url) {
        this.urlList.add(url);
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setRetweetCount(String retweet_count) {
        this.retweet_count = retweet_count;
    }

    public void setFavoriteCount(String favorite_count) {
        this.favorite_count = favorite_count;
    }

    public void setContributors(String contributors) {
        this.contributors = contributors;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public void setGeo(String geo) {
        this.geo = geo;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setCreatedAt(String created_at) {
        this.created_at = created_at;
    }

    public void setPlace(String place) {
        this.place = place;
    }

    public void setRetweeted(String retweeted) {
        this.retweeted = retweeted;
    }

    public void setTruncated(String truncated) {
        this.truncated = truncated;
    }

    public void setFavorited(String favorited) {
        this.favorited = favorited;
    }

    public void setCoordinates(String coordinates) {
        this.coordinates = coordinates;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    /* getters */
    public String getId() {
        return this.id;
    }

    public String getText() {
        return this.text;
    }

    public String getUrls() {
        if (urlList.isEmpty()) {
            return null;
        }
        int i = urlList.size();
        String arr = "[";
        for (String s : urlList) {
            arr += s;
            if (i>1) {
                arr += ",";
            }
            i--;
        }
        arr += "]";

        return arr;
    }

    public String getRetweetCount() {
        return this.retweet_count;
    }

    public String getFavoriteCount() {
        return this.favorite_count;
    }

    public String getContributors() {
        return this.contributors;
    }

    public String getLang() {
        return this.lang;
    }

    public String getGeo() {
        return this.geo;
    }

    public String getSource() {
        return this.source;
    }

    public String getCreatedAt() {
        return this.created_at;
    }

    public String getPlace() {
        return this.place;
    }

    public String getRetweeted() {
        return this.retweeted;
    }

    public String getTruncated() {
        return this.truncated;
    }

    public String getFavorited() {
        return this.favorited;
    }

    public String getCoordinates() {
        return this.coordinates;
    }

    public String getKeyword() {
        return this.keyword;
    }

    public JsonTweetUser getUser() {
        return this.profile;
    }
}
