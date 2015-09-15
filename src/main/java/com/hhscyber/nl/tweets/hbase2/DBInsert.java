/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbase2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author eve
 */
public class DBInsert {
    private Configuration config;
    private HTable hTable;
    private ArrayList<JsonTweet> twar;
    
    public DBInsert(String table) {
        this.config = HBaseConfiguration.create();
        try {
            this.hTable = new HTable(config, table);
        } catch (IOException ex) {
            Logger.getLogger(DBInsert.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void setDataArray(ArrayList<JsonTweet> ar) {
        this.twar = ar;
    }

    public void close() {
        try {
            hTable.close();
        } catch (IOException ex) {
            Logger.getLogger(DBInsert.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void doFlush() {
        int i = 0;
        List<Row> batch = new ArrayList<>();

        if (twar.isEmpty()) {
            return;
        }

        for (JsonTweet tw : twar) {
            Put p = new Put(Bytes.toBytes(tw.getId()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("id"), Bytes.toBytes(tw.getUser().getId()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("location"), Bytes.toBytes(tw.getUser().getLocation()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("default_profile"), Bytes.toBytes(tw.getUser().getDefault_profile()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("profile_backround_tile"), Bytes.toBytes(tw.getUser().getProfile_background_tile()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("statuses_count"), Bytes.toBytes(tw.getUser().getStatuses_count()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("lang"), Bytes.toBytes(tw.getUser().getLang()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("following"), Bytes.toBytes(tw.getUser().getFollowing()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("protected"), Bytes.toBytes(tw.getUser().getProtected()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("favorites_count"), Bytes.toBytes(tw.getUser().getFavourites_count()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("description"), Bytes.toBytes(tw.getUser().getDescription()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("verified"), Bytes.toBytes(tw.getUser().getVerified()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("contributors_enabled"), Bytes.toBytes(tw.getUser().getContributors_enabled()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("name"), Bytes.toBytes(tw.getUser().getName()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("created_at"), Bytes.toBytes(tw.getUser().getCreated_at()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("is_translation_enabled"), Bytes.toBytes(tw.getUser().getIs_translation_enabled()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("default_profile_image"), Bytes.toBytes(tw.getUser().getDefault_profile_image()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("followers_count"), Bytes.toBytes(tw.getUser().getFollowers_count()));
            if (tw.getUser().getHas_extended_profile() != null)
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("has_extended_profile"), Bytes.toBytes(tw.getUser().getHas_extended_profile()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("profile_image_url_https"), Bytes.toBytes(tw.getUser().getProfile_image_url_https()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("geo_enabled"), Bytes.toBytes(tw.getUser().getGeo_enabled()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("profile_background_image_url_https"), Bytes.toBytes(tw.getUser().getProfile_background_image_url_https()));
            if (tw.getUser().getUrl() != null)
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("url"), Bytes.toBytes(tw.getUser().getUrl()));
            if (tw.getUser().getUtc_offset() != null)
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("utc_offset"), Bytes.toBytes(tw.getUser().getUtc_offset()));
            if (tw.getUser().getTime_zone() != null)
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("time_zone"), Bytes.toBytes(tw.getUser().getTime_zone()));
            if (tw.getUser().getNotifications() != null)
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("notifications"), Bytes.toBytes(tw.getUser().getNotifications()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("friends_count"), Bytes.toBytes(tw.getUser().getFriends_count()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("screen_name"), Bytes.toBytes(tw.getUser().getScreen_name()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("listed_count"), Bytes.toBytes(tw.getUser().getListed_count()));
            p.add(Bytes.toBytes("profile"), Bytes.toBytes("is_translator"), Bytes.toBytes(tw.getUser().getIs_translator()));

            p.add(Bytes.toBytes("content"), Bytes.toBytes("text"), Bytes.toBytes(tw.getText()));
            p.add(Bytes.toBytes("content"), Bytes.toBytes("favorite_count"), Bytes.toBytes(tw.getFavoriteCount()));
            p.add(Bytes.toBytes("content"), Bytes.toBytes("retweet_count"), Bytes.toBytes(tw.getRetweetCount()));
            if (tw.getContributors() != null)
                p.add(Bytes.toBytes("content"), Bytes.toBytes("contributors"), Bytes.toBytes(tw.getContributors()));
            if (tw.getCoordinates() != null)
                p.add(Bytes.toBytes("content"), Bytes.toBytes("coordinated"), Bytes.toBytes(tw.getCoordinates()));
            p.add(Bytes.toBytes("content"), Bytes.toBytes("created_at"), Bytes.toBytes(tw.getCreatedAt()));
            p.add(Bytes.toBytes("content"), Bytes.toBytes("favorited"), Bytes.toBytes(tw.getFavorited()));
            if (tw.getGeo() != null)
                p.add(Bytes.toBytes("content"), Bytes.toBytes("geo"), Bytes.toBytes(tw.getGeo()));
            p.add(Bytes.toBytes("content"), Bytes.toBytes("truncated"), Bytes.toBytes(tw.getTruncated()));
            if (tw.getPlace() != null)
                p.add(Bytes.toBytes("content"), Bytes.toBytes("place"), Bytes.toBytes(tw.getPlace()));
            p.add(Bytes.toBytes("content"), Bytes.toBytes("source"), Bytes.toBytes(tw.getSource()));
            p.add(Bytes.toBytes("content"), Bytes.toBytes("lang"), Bytes.toBytes(tw.getLang()));
            p.add(Bytes.toBytes("content"), Bytes.toBytes("retweeted"), Bytes.toBytes(tw.getRetweeted()));

            batch.add(p);
            i++;
        }
        try {
            hTable.batch(batch);
        } catch (InterruptedException | IOException ex) {
            Logger.getLogger(DBInsert.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println(i+" tweets flushed to database");
    }

}
