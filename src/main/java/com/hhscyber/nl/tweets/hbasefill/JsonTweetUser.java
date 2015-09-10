/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.hbasefill;

/**
 *
 * @author eve
 */
public class JsonTweetUser {

    private final String id;
    private String location;
    private String default_profile;
    private String profile_background_tile;
    private String statuses_count;
    private String lang;
    private String following;
    private String uprotected;
    private String favourites_count;
    private String description;
    private String verified;
    private String contributors_enabled;
    private String name;
    private String created_at;
    private String is_translation_enabled;
    private String default_profile_image;
    private String followers_count;
    private String has_extended_profile;
    private String profile_image_url_https;
    private String geo_enabled;
    private String profile_background_image_url_https;
    private String url;
    private String utc_offset;
    private String time_zone;
    private String notifications;
    private String friends_count;
    private String screen_name;
    private String listed_count;
    private String is_translator;

    public JsonTweetUser(String id) {
        this.id = id;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setDefaultProfile(String default_profile) {
        this.default_profile = default_profile;
    }

    public void setBackgroundTile(String profile_background_tile) {
        this.profile_background_tile = profile_background_tile;
    }

    public void setStatusesCount(String statuses_count) {
        this.statuses_count = statuses_count;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public void setFollowing(String following) {
        this.following = following;
    }

    public void setProtected(String uprotected) {
        this.uprotected = uprotected;
    }

    public void setFavoriteCount(String favourites_count) {
        this.favourites_count = favourites_count;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setVerified(String verified) {
        this.verified = verified;
    }

    public void setContributorsEnabled(String contributors_enabled) {
        this.contributors_enabled = contributors_enabled;
    }

    public void setname(String name) {
        this.name = name;
    }

    public void setCreatedAt(String created_at) {
        this.created_at = created_at;
    }

    public void setTranslationEnabled(String is_translation_enabled) {
        this.is_translation_enabled = is_translation_enabled;
    }

    public void setDefaultProfileImg(String default_profile_image) {
        this.default_profile_image = default_profile_image;
    }

    public void setFollowerCount(String followers_count) {
        this.followers_count = followers_count;
    }

    public void setExtendedProfile(String has_extended_profile) {
        this.has_extended_profile = has_extended_profile;
    }

    public void setProfileImg(String profile_image_url_https) {
        this.profile_image_url_https = profile_image_url_https;
    }

    public void setGeoEnabled(String geo_enabled) {
        this.geo_enabled = geo_enabled;
    }

    public void setProfileBackgroundImg(String profile_background_image_url_https) {
        this.profile_background_image_url_https = profile_background_image_url_https;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUtcOffset(String utc_offset) {
        this.utc_offset = utc_offset;
    }

    public void setTimeZone(String time_zone) {
        this.time_zone = time_zone;
    }

    public void setNotifications(String notifications) {
        this.notifications = notifications;
    }

    public void setFiendCount(String friends_count) {
        this.friends_count = friends_count;
    }

    public void setScreenName(String screen_name) {
        this.screen_name = screen_name;
    }

    public void setListedCount(String listed_count) {
        this.listed_count = listed_count;
    }

    public void setIsTranslator(String is_translator) {
        this.is_translator = is_translator;
    }

    /* Getters */
    public String getId() {
        return id;
    }

    public String getLocation() {
        return location;
    }

    public String getDefault_profile() {
        return default_profile;
    }

    public String getProfile_background_tile() {
        return profile_background_tile;
    }

    public String getStatuses_count() {
        return statuses_count;
    }

    public String getLang() {
        return lang;
    }

    public String getFollowing() {
        return following;
    }

    public String getProtected() {
        return uprotected;
    }

    public String getFavourites_count() {
        return favourites_count;
    }

    public String getDescription() {
        return description;
    }

    public String getVerified() {
        return verified;
    }

    public String getContributors_enabled() {
        return contributors_enabled;
    }

    public String getName() {
        return name;
    }

    public String getCreated_at() {
        return created_at;
    }

    public String getIs_translation_enabled() {
        return is_translation_enabled;
    }

    public String getDefault_profile_image() {
        return default_profile_image;
    }

    public String getFollowers_count() {
        return followers_count;
    }

    public String getHas_extended_profile() {
        return has_extended_profile;
    }

    public String getProfile_image_url_https() {
        return profile_image_url_https;
    }

    public String getGeo_enabled() {
        return geo_enabled;
    }

    public String getProfile_background_image_url_https() {
        return profile_background_image_url_https;
    }

    public String getUrl() {
        return url;
    }

    public String getUtc_offset() {
        return utc_offset;
    }

    public String getTime_zone() {
        return time_zone;
    }

    public String getNotifications() {
        return notifications;
    }

    public String getFriends_count() {
        return friends_count;
    }

    public String getScreen_name() {
        return screen_name;
    }

    public String getListed_count() {
        return listed_count;
    }

    public String getIs_translator() {
        return is_translator;
    }

}
