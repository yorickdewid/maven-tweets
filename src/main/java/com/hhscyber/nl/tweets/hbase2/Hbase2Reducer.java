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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

/**
 *
 * @author eve
 */
public class Hbase2Reducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

    private ArrayList<JsonTweet> tweets;

    @Override
    public void setup(Context context) throws IOException {
        this.tweets = new ArrayList<>();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        try {
            for (Text value : values) {
                parseJSON(value.toString());
            }

            for (JsonTweet tw : tweets) {
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
                if (tw.getUser().getHas_extended_profile() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("has_extended_profile"), Bytes.toBytes(tw.getUser().getHas_extended_profile()));
                }
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("profile_image_url_https"), Bytes.toBytes(tw.getUser().getProfile_image_url_https()));
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("geo_enabled"), Bytes.toBytes(tw.getUser().getGeo_enabled()));
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("profile_background_image_url_https"), Bytes.toBytes(tw.getUser().getProfile_background_image_url_https()));
                if (tw.getUser().getUrl() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("url"), Bytes.toBytes(tw.getUser().getUrl()));
                }
                if (tw.getUser().getUtc_offset() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("utc_offset"), Bytes.toBytes(tw.getUser().getUtc_offset()));
                }
                if (tw.getUser().getTime_zone() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("time_zone"), Bytes.toBytes(tw.getUser().getTime_zone()));
                }
                if (tw.getUser().getNotifications() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("notifications"), Bytes.toBytes(tw.getUser().getNotifications()));
                }
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("friends_count"), Bytes.toBytes(tw.getUser().getFriends_count()));
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("screen_name"), Bytes.toBytes(tw.getUser().getScreen_name()));
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("listed_count"), Bytes.toBytes(tw.getUser().getListed_count()));
                p.add(Bytes.toBytes("profile"), Bytes.toBytes("is_translator"), Bytes.toBytes(tw.getUser().getIs_translator()));

                p.add(Bytes.toBytes("content"), Bytes.toBytes("text"), Bytes.toBytes(tw.getText()));
                p.add(Bytes.toBytes("content"), Bytes.toBytes("favorite_count"), Bytes.toBytes(tw.getFavoriteCount()));
                p.add(Bytes.toBytes("content"), Bytes.toBytes("retweet_count"), Bytes.toBytes(tw.getRetweetCount()));
                if (tw.getContributors() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("contributors"), Bytes.toBytes(tw.getContributors()));
                }
                if (tw.getCoordinates() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("coordinated"), Bytes.toBytes(tw.getCoordinates()));
                }
                p.add(Bytes.toBytes("content"), Bytes.toBytes("created_at"), Bytes.toBytes(tw.getCreatedAt()));
                p.add(Bytes.toBytes("content"), Bytes.toBytes("favorited"), Bytes.toBytes(tw.getFavorited()));
                if (tw.getGeo() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("geo"), Bytes.toBytes(tw.getGeo()));
                }
                p.add(Bytes.toBytes("content"), Bytes.toBytes("truncated"), Bytes.toBytes(tw.getTruncated()));
                if (tw.getPlace() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("place"), Bytes.toBytes(tw.getPlace()));
                }
                p.add(Bytes.toBytes("content"), Bytes.toBytes("source"), Bytes.toBytes(tw.getSource()));
                p.add(Bytes.toBytes("content"), Bytes.toBytes("lang"), Bytes.toBytes(tw.getLang()));
                p.add(Bytes.toBytes("content"), Bytes.toBytes("retweeted"), Bytes.toBytes(tw.getRetweeted()));

                context.write(null, p);

            }
        } catch (IOException | InterruptedException ex) {
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
