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
                if (tw.getUser().getId() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("id"), Bytes.toBytes(tw.getUser().getId()));
                }
                if (tw.getUser().getLocation() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("location"), Bytes.toBytes(tw.getUser().getLocation()));
                }
                if (tw.getUser().getDefault_profile() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("default_profile"), Bytes.toBytes(tw.getUser().getDefault_profile()));
                }
                if (tw.getUser().getProfile_background_tile() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("profile_backround_tile"), Bytes.toBytes(tw.getUser().getProfile_background_tile()));
                }
                if (tw.getUser().getStatuses_count() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("statuses_count"), Bytes.toBytes(tw.getUser().getStatuses_count()));
                }
                if (tw.getUser().getLang() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("lang"), Bytes.toBytes(tw.getUser().getLang()));
                }
                if (tw.getUser().getFollowing() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("following"), Bytes.toBytes(tw.getUser().getFollowing()));
                }
                if (tw.getUser().getProtected() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("protected"), Bytes.toBytes(tw.getUser().getProtected()));
                }
                if (tw.getUser().getFavourites_count() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("favorites_count"), Bytes.toBytes(tw.getUser().getFavourites_count()));
                }
                if (tw.getUser().getDescription() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("description"), Bytes.toBytes(tw.getUser().getDescription()));
                }
                if (tw.getUser().getVerified() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("verified"), Bytes.toBytes(tw.getUser().getVerified()));
                }
                if (tw.getUser().getContributors_enabled() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("contributors_enabled"), Bytes.toBytes(tw.getUser().getContributors_enabled()));
                }
                if (tw.getUser().getName() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("name"), Bytes.toBytes(tw.getUser().getName()));
                }
                if (tw.getUser().getCreated_at() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("created_at"), Bytes.toBytes(tw.getUser().getCreated_at()));
                }
                if (tw.getUser().getIs_translation_enabled() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("is_translation_enabled"), Bytes.toBytes(tw.getUser().getIs_translation_enabled()));
                }
                if (tw.getUser().getDefault_profile_image() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("default_profile_image"), Bytes.toBytes(tw.getUser().getDefault_profile_image()));
                }
                if (tw.getUser().getFollowers_count() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("followers_count"), Bytes.toBytes(tw.getUser().getFollowers_count()));
                }
                if (tw.getUser().getHas_extended_profile() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("has_extended_profile"), Bytes.toBytes(tw.getUser().getHas_extended_profile()));
                }
                if (tw.getUser().getProfile_image_url_https() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("profile_image_url_https"), Bytes.toBytes(tw.getUser().getProfile_image_url_https()));
                }
                if (tw.getUser().getGeo_enabled() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("geo_enabled"), Bytes.toBytes(tw.getUser().getGeo_enabled()));
                }
                if (tw.getUser().getProfile_background_image_url_https() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("profile_background_image_url_https"), Bytes.toBytes(tw.getUser().getProfile_background_image_url_https()));
                }
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
                if (tw.getUser().getFriends_count() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("friends_count"), Bytes.toBytes(tw.getUser().getFriends_count()));
                }
                if (tw.getUser().getScreen_name() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("screen_name"), Bytes.toBytes(tw.getUser().getScreen_name()));
                }
                if (tw.getUser().getListed_count() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("listed_count"), Bytes.toBytes(tw.getUser().getListed_count()));
                }
                if (tw.getUser().getIs_translator() != null) {
                    p.add(Bytes.toBytes("profile"), Bytes.toBytes("is_translator"), Bytes.toBytes(tw.getUser().getIs_translator()));
                }

                if (tw.getText() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("text"), Bytes.toBytes(tw.getText()));
                }
                if (tw.getKeyword() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("keyword"), Bytes.toBytes(tw.getKeyword()));
                }
                if (tw.getFavoriteCount() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("favorite_count"), Bytes.toBytes(tw.getFavoriteCount()));
                }
                if (tw.getRetweetCount() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("retweet_count"), Bytes.toBytes(tw.getRetweetCount()));
                }
                if (tw.getContributors() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("contributors"), Bytes.toBytes(tw.getContributors()));
                }
                if (tw.getCoordinates() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("coordinated"), Bytes.toBytes(tw.getCoordinates()));
                }
                if (tw.getCreatedAt() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("created_at"), Bytes.toBytes(tw.getCreatedAt()));
                }
                if (tw.getFavorited() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("favorited"), Bytes.toBytes(tw.getFavorited()));
                }
                if (tw.getGeo() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("geo"), Bytes.toBytes(tw.getGeo()));
                }
                if (tw.getTruncated() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("truncated"), Bytes.toBytes(tw.getTruncated()));
                }
                if (tw.getPlace() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("place"), Bytes.toBytes(tw.getPlace()));
                }
                if (tw.getSource() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("source"), Bytes.toBytes(tw.getSource()));
                }
                if (tw.getLang() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("lang"), Bytes.toBytes(tw.getLang()));
                }
                if (tw.getRetweeted() != null) {
                    p.add(Bytes.toBytes("content"), Bytes.toBytes("retweeted"), Bytes.toBytes(tw.getRetweeted()));
                }

                context.write(null, p);

            }
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(ConcatTweetsReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void parseJSON(String singleLine) {
        JSONObject json = (JSONObject) JSONValue.parse(singleLine);
        String keyword = (String) json.get("class");

        JSONObject data = (JSONObject) json.get("data");
        JSONArray jry = (JSONArray) data.get("statuses");
        if (jry == null) {
            return;
        }

        for (Object jry1 : jry) {
            JSONObject obj, pobj;
            obj = (JSONObject) jry1;
            pobj = (JSONObject) obj.get("user");
            JsonTweet jt = new JsonTweet(obj.get("id_str").toString(), pobj.get("id_str").toString());
            jt.setText(obj.get("text").toString());
            jt.setKeyword(keyword);
            if (obj.get("retweet_count") != null) {
                jt.setRetweetCount(obj.get("retweet_count").toString());
            }
            if (obj.get("favorite_count") != null) {
                jt.setFavoriteCount(obj.get("favorite_count").toString());
            }
            if (obj.get("contributors") != null) {
                jt.setContributors(obj.get("contributors").toString());
            }
            if (obj.get("lang") != null) {
                jt.setLang(obj.get("lang").toString());
            }
            if (obj.get("geo") != null) {
                jt.setGeo(obj.get("geo").toString());
            }
            if (obj.get("source") != null) {
                jt.setSource(obj.get("source").toString());
            }
            if (obj.get("created_at") != null) {
                jt.setCreatedAt(obj.get("created_at").toString());
            }
            if (obj.get("place") != null) {
                jt.setPlace(obj.get("place").toString());
            }
            if (obj.get("retweeted") != null) {
                jt.setRetweeted(obj.get("retweeted").toString());
            }
            if (obj.get("truncated") != null) {
                jt.setTruncated(obj.get("truncated").toString());
            }
            if (obj.get("favorited") != null) {
                jt.setFavorited(obj.get("favorited").toString());
            }
            if (obj.get("coordinates") != null) {
                jt.setCoordinates(obj.get("coordinates").toString());
            }
            JsonTweetUser jtu = jt.getUser();
            if (pobj.get("location") != null) {
                jtu.setLocation(pobj.get("location").toString());
            }
            if (pobj.get("default_profile") != null) {
                jtu.setDefaultProfile(pobj.get("default_profile").toString());
            }
            if (pobj.get("profile_background_tile") != null) {
                jtu.setBackgroundTile(pobj.get("profile_background_tile").toString());
            }
            if (pobj.get("statuses_count") != null) {
                jtu.setStatusesCount(pobj.get("statuses_count").toString());
            }
            if (pobj.get("lang") != null) {
                jtu.setLang(pobj.get("lang").toString());
            }
            if (pobj.get("following") != null) {
                jtu.setFollowing(pobj.get("following").toString());
            }
            if (pobj.get("protected") != null) {
                jtu.setProtected(pobj.get("protected").toString());
            }
            if (pobj.get("favourites_count") != null) {
                jtu.setFavoriteCount(pobj.get("favourites_count").toString());
            }
            if (pobj.get("description") != null) {
                jtu.setDescription(pobj.get("description").toString());
            }
            if (pobj.get("verified") != null) {
                jtu.setVerified(pobj.get("verified").toString());
            }
            if (pobj.get("contributors_enabled") != null) {
                jtu.setContributorsEnabled(pobj.get("contributors_enabled").toString());
            }
            if (pobj.get("name") != null) {
                jtu.setname(pobj.get("name").toString());
            }
            if (pobj.get("created_at") != null) {
                jtu.setCreatedAt(pobj.get("created_at").toString());
            }
            if (pobj.get("is_translation_enabled") != null) {
                jtu.setTranslationEnabled(pobj.get("is_translation_enabled").toString());
            }
            if (pobj.get("default_profile_image") != null) {
                jtu.setDefaultProfileImg(pobj.get("default_profile_image").toString());
            }
            if (pobj.get("followers_count") != null) {
                jtu.setFollowerCount(pobj.get("followers_count").toString());
            }
            if (pobj.get("has_extended_profile") != null) {
                jtu.setExtendedProfile(pobj.get("has_extended_profile").toString());
            }
            if (pobj.get("profile_image_url_https") != null) {
                jtu.setProfileImg(pobj.get("profile_image_url_https").toString());
            }
            if (pobj.get("geo_enabled") != null) {
                jtu.setGeoEnabled(pobj.get("geo_enabled").toString());
            }
            if (pobj.get("profile_background_image_url_https") != null) {
                jtu.setProfileBackgroundImg(pobj.get("profile_background_image_url_https").toString());
            }
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
            if (pobj.get("friends_count") != null) {
                jtu.setFiendCount(pobj.get("friends_count").toString());
            }
            if (pobj.get("screen_name") != null) {
                jtu.setScreenName(pobj.get("screen_name").toString());
            }
            if (pobj.get("listed_count") != null) {
                jtu.setListedCount(pobj.get("listed_count").toString());
            }
            if (pobj.get("is_translator") != null) {
                jtu.setIsTranslator(pobj.get("is_translator").toString());
            }
            tweets.add(jt);
        }
    }
}
