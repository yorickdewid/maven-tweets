/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.count;

/**
 *
 * @author dev
 */
public class countObjectLocation extends countObject {
    public int knownLocations = 0;
    public int unknownLocations = 0;
    public int totalLocations = 0;
    public int amountOfCities = 0;
    public int amountOfCounties = 0;
    public int amountOfCountries = 0;
    public int amountOfGeo = 0;
    public int amountOfState = 0;
    

    @Override
    public String toString() {
        String s = "Known locations : "+this.knownLocations;
        s += "\n";
        s += "Unknown Locations : " + this.unknownLocations;
        s += "\n";
        s += "Total Locations " + this.totalLocations;
        return s;
    }
    
    public String getOptionalFields(){
        String s = "Amount of cities : "+this.amountOfCities;
        s += "\n";
        s += "Amount of counties : " + this.amountOfCounties;
        s += "\n";
        s += "Amount of countries : " + this.amountOfCountries;
        s += "\n";
        s += "Amount of latitudes : " + this.amountOfGeo;
        s += "\n";
        s += "Amount of states : " + this.amountOfState;
        return s;
    }
    
}
