/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.location;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hhscyber.nl.carmen.LocationResolver;
import com.hhscyber.nl.carmen.types.Location;
import java.io.IOException;
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
    
    /*
     Read json directly 
    */
    private static void normalHadoop(){
        
    }
    
    /** 
     * Trick to not modify existing code/might be slower though
     * @param key
     * @param result 
     */
    private static Location buildJSONLocation(ImmutableBytesWritable key, Result result)
    {
        byte[] b = hbasehelper.HbaseHelper.getValueSafe(result,"profile","utc_offset");
        byte[] b2 = hbasehelper.HbaseHelper.getValueSafe(result,"profile","location");
        byte[] b3 = hbasehelper.HbaseHelper.getValueSafe(result,"content","geo");
        byte[] b4 = hbasehelper.HbaseHelper.getValueSafe(result,"profile","geo_enabled");
        /*
            
            PLACE object skipped
            USER object - GEO_enabled - LOCATION - utc_offset
            Root object geo code
            
            */
        JSONObject json = new JSONObject();
        String offset = hbasehelper.HbaseHelper.createStringFromByte(b);
        String location = hbasehelper.HbaseHelper.createStringFromByte(b2);
        String geo = hbasehelper.HbaseHelper.createStringFromByte(b3);
        String geo_enabled  = hbasehelper.HbaseHelper.createStringFromByte(b4);
        JSONObject user = new JSONObject();
        JSONObject entities = new JSONObject();
        entities.put("utc_offset", offset);
        user.put("location",location);
        user.put("geo_enabled",geo_enabled);
        json.put("user",user);
        json.put("geo",geo);
        json.put("entities",entities);
        try {
            return getLocation(json);
        } catch (IOException ex) {
            Logger.getLogger(LocationMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

     private static Location getLocation(JSONObject json) throws IOException{
        // Parse the command line.
        LocationResolver resolver = LocationResolver.getLocationResolver();

        ObjectMapper mapper = new ObjectMapper();

        @SuppressWarnings("unchecked")
        HashMap<String, Object> tweet = (HashMap<String, Object>) mapper.readValue(json.toString(), Map.class);

        total++;
        Location location = resolver.resolveLocationFromTweet(tweet);

        if (location != null) {
            
            //System.out.println("Found location: " + location.toString());
            if(location.getCountry() != null && location.getCity() != null)
            {
                numResolved++;
            }
            return location;
        }
        return null;
       //System.out.println("Resolved locations for " + numResolved + " of " + total + " tweets.");
    }

    
    private static Put resultToPut(ImmutableBytesWritable key, Result result,Location location) throws IOException {
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
            put.add(Bytes.toBytes("location"), Bytes.toBytes("known"), hbasehelper.HbaseHelper.getPutBytesSafe(location.isKnownLocation()));
            return put;

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
          System.out.println("Resolved locations for " + numResolved + " of " + total + " tweets.");
    }
    
}
