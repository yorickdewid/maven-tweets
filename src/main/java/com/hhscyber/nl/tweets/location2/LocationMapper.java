/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.location2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hhscyber.nl.carmen.LocationResolver;
import com.hhscyber.nl.carmen.types.Location;
import com.javadocmd.simplelatlng.LatLng;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.minidev.json.JSONObject;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author eve
 */
public class LocationMapper extends TableMapper<ImmutableBytesWritable, Put> {

    static int numResolved;
    static int total;

    @Override
    public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
        Location location = buildJSONLocation(row, value);
        Put p = resultToPut(row, value, location);
        if (p != null) {
            context.write(row, p);
        }
    }

    /**
     * Trick to not modify existing code/might be slower though
     *
     * @param key
     * @param result
     */
    private static Location buildJSONLocation(ImmutableBytesWritable key, Result result) {
        byte[] b = hbasehelper.HbaseHelper.getValueSafe(result, "profile", "utc_offset");
        byte[] b2 = hbasehelper.HbaseHelper.getValueSafe(result, "profile", "location");
        byte[] b3 = hbasehelper.HbaseHelper.getValueSafe(result, "content", "geo");
        byte[] b4 = hbasehelper.HbaseHelper.getValueSafe(result, "profile", "geo_enabled");
        byte[] b5 = hbasehelper.HbaseHelper.getValueSafe(result, "content", "coordinated");
        /*
            
         PLACE object skipped
         USER object - GEO_enabled - LOCATION - utc_offset
         Root object geo code
            
         */
        JSONObject json = new JSONObject();
        String offset = hbasehelper.HbaseHelper.createStringFromByte(b);
        String location = hbasehelper.HbaseHelper.createStringFromByte(b2);
        String geo = hbasehelper.HbaseHelper.createStringFromByte(b3);
        String geo_enabled = hbasehelper.HbaseHelper.createStringFromByte(b4);
        String time_zone = hbasehelper.HbaseHelper.createStringFromRawHbase(result, "profile", "time_zone");
        String coordinates = hbasehelper.HbaseHelper.createStringFromByte(b5);
        if (coordinates.equals("")) //prevent map classcast exception 
        {
            coordinates = "{\"type\":\"Point\",\"coordinates\":[0,0]}";
        }
        JSONObject user = new JSONObject();
        JSONObject entities = new JSONObject();
        entities.put("utc_offset", offset);
        user.put("location", location);
        user.put("geo_enabled", geo_enabled);
        user.put("time_zone", time_zone);
        json.put("user", user);
        json.put("geo", geo);
        json.put("coordinates", coordinates);
        json.put("entities", entities);
        try {
            return getLocation(json);
        } catch (IOException ex) {
            Logger.getLogger(LocationMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private static Location getLocation(JSONObject json) throws IOException {
        // Parse the command line.
        LocationResolver resolver = LocationResolver.getLocationResolver();

        ObjectMapper mapper = new ObjectMapper();

        @SuppressWarnings("unchecked")
        HashMap<String, Object> tweet = (HashMap<String, Object>) mapper.readValue(json.toString(), Map.class);

        total++;
        ArrayList<Location> locations = resolver.resolveLocationFromTweetCombineAll(tweet);
        Double[][][] geo = new Double[locations.size()][1][2];
        ArrayList<String> cities = new ArrayList<>();
        ArrayList<String> counties = new ArrayList<>();
        ArrayList<String> countries = new ArrayList<>();
        ArrayList<String> states = new ArrayList<>();
        ArrayList<Boolean> known = new ArrayList<>();
        if (locations.size() >= 1) {
            for (int i = 0; i < locations.size(); i++) {
                //System.out.println("Found location: " + location.toString());
                Location location = locations.get(i);
                LatLng tmp = location.getLatLng();
                geo[i][0][0] = tmp.getLatitude();
                geo[i][0][1] = tmp.getLongitude();
                cities.add(location.getCity());
                counties.add(location.getCounty());
                countries.add(location.getCountry());
                states.add(location.getState());
                known.add(location.isKnownLocation());
            }
        }
        LocationObject location = new LocationObject();
        location.setGeo(geo);
        location.setCities(cities);
        location.setCounties(counties);
        location.setCountries(countries);
        location.setStates(states);
        location.setKnown(known);
        identifyMostAccurate(location);
        return null;
        //System.out.println("Resolved locations for " + numResolved + " of " + total + " tweets.");
    }

    private static Location identifyMostAccurate(LocationObject location) {
        int countPoint = 0;
        for(boolean known : location.getKnown())
        {
            if(known)
                countPoint++;
        }
        countPoint += countPointsForNotEmpty(location.getCities());
        countPoint += countPointsForNotEmpty(location.getCounties());
        countPoint += countPointsForNotEmpty(location.getCountries());
        countPoint += countPointsForNotEmpty(location.getStates());
        return null;
    }

    private static int countPointsForNotEmpty(ArrayList<String> generics){
        int countPoints = 0;
        for(String generic : generics)
        {
            if(!generic.equalsIgnoreCase(""))
                countPoints++;
        }
        return countPoints;
    }
    
    private static Put resultToPut(ImmutableBytesWritable key, Result result, Location location) throws IOException {
        Put put = new Put(key.get());
        //city
        //county
        //state
        // location_country
        // known_location
        for (Cell c : result.rawCells()) {
            put.add(c);
        }
        put.add(Bytes.toBytes("location"), Bytes.toBytes("city"), hbasehelper.HbaseHelper.getPutBytesSafe(location.getCity()));
        put.add(Bytes.toBytes("location"), Bytes.toBytes("county"), hbasehelper.HbaseHelper.getPutBytesSafe(location.getCounty()));
        put.add(Bytes.toBytes("location"), Bytes.toBytes("state"), hbasehelper.HbaseHelper.getPutBytesSafe(location.getState()));
        put.add(Bytes.toBytes("location"), Bytes.toBytes("country"), hbasehelper.HbaseHelper.getPutBytesSafe(location.getCountry()));
        put.add(Bytes.toBytes("location"), Bytes.toBytes("latitude"), hbasehelper.HbaseHelper.getPutBytesSafe(String.valueOf(location.getLatLng().getLatitude())));
        put.add(Bytes.toBytes("location"), Bytes.toBytes("longitude"), hbasehelper.HbaseHelper.getPutBytesSafe(String.valueOf(location.getLatLng().getLongitude())));
        put.add(Bytes.toBytes("location"), Bytes.toBytes("known"), hbasehelper.HbaseHelper.getPutBytesSafe(location.isKnownLocation()));
        return put;

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Resolved locations for " + numResolved + " of " + total + " tweets.");
    }

}
