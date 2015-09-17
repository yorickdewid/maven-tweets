/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbase2;

import com.hhscyber.nl.tweets.concattweets.ConcatTweetsReducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author eve
 */
public class Hbase2Reducer extends Reducer<Text, Text, Text, IntWritable> {

    private ArrayList<JsonTweet> tweets;
    private HConnection connection;

    public void setup(Context context) throws IOException {
        this.tweets = new ArrayList<>();
        connection = HConnectionManager.createConnection(context.getConfiguration());
     }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        try {
            for (Text value : values) {
                parseJSON(value.toString());
            }
            HTableInterface table = connection.getTable("hhscyber:tweets_test");
            //iets met table.put ???
            byte[] tableName = table.getTableName();
            System.out.println("tablename " + new String(tableName));
            
            
            //DBInsert db = new DBInsert("hhscyber:tweets_test");
            //db.setDataArray(tweets);
            //db.doFlush();
            //db.close();
            tweets.clear();
        } catch (Exception ex) {
            Logger.getLogger(ConcatTweetsReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void parseJSON(String singleLine) {
        JSONObject json = (JSONObject) JSONValue.parse(singleLine);
        JSONArray jry = (JSONArray) json.get("statuses");
        if (jry == null) {
            return;
        }

        for (Object jry1 : jry) {
            JSONObject obj, pobj;
            obj = (JSONObject) jry1;
            pobj = (JSONObject) obj.get("user");
            JsonTweet jt = new JsonTweet(obj.get("id_str").toString(), pobj.get("id_str").toString());
            jt.setText(obj.get("text").toString());
            jt.setRetweetCount(obj.get("retweet_count").toString());
            jt.setFavoriteCount(obj.get("favorite_count").toString());
            if (obj.get("contributors") != null) {
                jt.setContributors(obj.get("contributors").toString());
            }
            jt.setLang(obj.get("lang").toString());
            if (obj.get("geo") != null) {
                jt.setGeo(obj.get("geo").toString());
            }
            jt.setSource(obj.get("source").toString());
            jt.setCreatedAt(obj.get("created_at").toString());
            if (obj.get("place") != null) {
                jt.setPlace(obj.get("place").toString());
            }
            jt.setRetweeted(obj.get("retweeted").toString());
            jt.setTruncated(obj.get("truncated").toString());
            jt.setFavorited(obj.get("favorited").toString());
            if (obj.get("coordinates") != null) {
                jt.setCoordinates(obj.get("coordinates").toString());
            }
            JsonTweetUser jtu = jt.getUser();
            jtu.setLocation(pobj.get("location").toString());
            jtu.setDefaultProfile(pobj.get("default_profile").toString());
            jtu.setBackgroundTile(pobj.get("profile_background_tile").toString());
            jtu.setStatusesCount(pobj.get("statuses_count").toString());
            jtu.setLang(pobj.get("lang").toString());
            jtu.setFollowing(pobj.get("following").toString());
            jtu.setProtected(pobj.get("protected").toString());
            jtu.setFavoriteCount(pobj.get("favourites_count").toString());
            jtu.setDescription(pobj.get("description").toString());
            jtu.setVerified(pobj.get("verified").toString());
            jtu.setContributorsEnabled(pobj.get("contributors_enabled").toString());
            jtu.setname(pobj.get("name").toString());
            jtu.setCreatedAt(pobj.get("created_at").toString());
            jtu.setTranslationEnabled(pobj.get("is_translation_enabled").toString());
            jtu.setDefaultProfileImg(pobj.get("default_profile_image").toString());
            jtu.setFollowerCount(pobj.get("followers_count").toString());
            if (pobj.get("has_extended_profile") != null) {
                jtu.setExtendedProfile(pobj.get("has_extended_profile").toString());
            }
            jtu.setProfileImg(pobj.get("profile_image_url_https").toString());
            jtu.setGeoEnabled(pobj.get("geo_enabled").toString());
            jtu.setProfileBackgroundImg(pobj.get("profile_background_image_url_https").toString());
            if (pobj.get("url") != null) {
                jtu.setUrl(pobj.get("url").toString());
            }
            if (pobj.get("utc_offset") != null) {
                jtu.setUtcOffset(pobj.get("utc_offset").toString());
            }
            if (pobj.get("time_zone") != null) {
                jtu.setTimeZone(pobj.get("time_zone").toString());
            }
            if (pobj.get("notifications") != null) {
                jtu.setNotifications(pobj.get("notifications").toString());
            }
            jtu.setFiendCount(pobj.get("friends_count").toString());
            jtu.setScreenName(pobj.get("screen_name").toString());
            jtu.setListedCount(pobj.get("listed_count").toString());
            jtu.setIsTranslator(pobj.get("is_translator").toString());
            tweets.add(jt);
        }
    }
}
