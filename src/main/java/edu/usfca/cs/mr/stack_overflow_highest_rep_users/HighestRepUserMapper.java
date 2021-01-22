package edu.usfca.cs.mr.stack_overflow_highest_rep_users;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Mapper: Reads line by line, split them into words. Emit <user, user-meta data containing count> pairs.
 */
public class HighestRepUserMapper
extends Mapper<LongWritable, Text, Text, LocationWritable> {

    private static Logger logger = LoggerFactory.getLogger(HighestRepUserMapper.class.getName());
    String[] countries;

    @Override
    protected void setup(Context context) {
        // Predefined list of countries
        countries = new String[] {"United Kingdom","United States","Afghanistan","Albania","Algeria","Andorra","Angola","Antigua & Deps","Argentina","Armenia","Australia","Austria","Azerbaijan","Bahamas","Bahrain","Bangladesh","Barbados","Belarus","Belgium","Belize","Benin","Bhutan","Bolivia","Bosnia Herzegovina","Botswana","Brazil","Brunei","Bulgaria","Burkina","Burundi","Cambodia","Cameroon","Canada","Cape Verde","Central African Rep","Chad","Chile","China","Colombia","Comoros","Congo","Congo {Democratic Rep}","Costa Rica","Croatia","Cuba","Cyprus","Czech Republic","Denmark","Djibouti","Dominica","Dominican Republic","East Timor","Ecuador","Egypt","El Salvador","Equatorial Guinea","Eritrea","Estonia","Ethiopia","Fiji","Finland","France","Gabon","Gambia","Georgia","Germany","Ghana","Greece","Grenada","Guatemala","Guinea","Guinea-Bissau","Guyana","Haiti","Honduras","Hungary","Iceland","India","Indonesia","Iran","Iraq","Ireland {Republic}","Israel","Italy","Ivory Coast","Jamaica","Japan","Jordan","Kazakhstan","Kenya","Kiribati","Korea North","Korea South","Kosovo","Kuwait","Kyrgyzstan","Laos","Latvia","Lebanon","Lesotho","Liberia","Libya","Liechtenstein","Lithuania","Luxembourg","Macedonia","Madagascar","Malawi","Malaysia","Maldives","Mali","Malta","Marshall Islands","Mauritania","Mauritius","Mexico","Micronesia","Moldova","Monaco","Mongolia","Montenegro","Morocco","Mozambique","Myanmar, {Burma}","Namibia","Nauru","Nepal","Netherlands","New Zealand","Nicaragua","Niger","Nigeria","Norway","Oman","Pakistan","Palau","Panama","Papua New Guinea","Paraguay","Peru","Philippines","Poland","Portugal","Qatar","Romania","Russian Federation","Rwanda","St Kitts & Nevis","St Lucia","Saint Vincent & the Grenadines","Samoa","San Marino","Sao Tome & Principe","Saudi Arabia","Senegal","Serbia","Seychelles","Sierra Leone","Singapore","Slovakia","Slovenia","Solomon Islands","Somalia","South Africa","South Sudan","Spain","Sri Lanka","Sudan","Suriname","Swaziland","Sweden","Switzerland","Syria","Taiwan","Tajikistan","Tanzania","Thailand","Togo","Tonga","Trinidad & Tobago","Tunisia","Turkey","Turkmenistan","Tuvalu","Uganda","Ukraine","United Arab Emirates","Uruguay","Uzbekistan","Vanuatu","Vatican City","Venezuela","Vietnam","Yemen","Zambia","Zimbabwe"};
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        try {
            String line = value.toString();
            // Check if line is invalid
            if(line == null || line.isEmpty() || line.trim().length() == 0)
                return;

            line = line.trim();

            // Checking if valid line as per our requirement
            if(line.startsWith("<row")){

                // Removing extra quotes (") from string
                line = line.replace("\"", "");

                // Extracting required metadata
                Text website = new Text("NA");
                Text location = null;
                Text displayName = null;
                int upvotes = 0;
                int views = 0;
                int reputation = 0;
                Text creationDate = null;

                String[] attributes = line.split(" ");

                // Extracting the meta data
                for(String attribute : attributes){
                    String[] attr = attribute.split("=");

                    if(attr.length > 1){

                        String attrKey = attr[0].trim();
                        String attrVal = attr[1].trim();
                        switch (attrKey){
                            case "WebsiteUrl":
                                website = new Text(attrVal);
                                break;
                            case "DisplayName":
                                displayName = new Text(attrVal);
                                break;
                            case "Location":
                                String locationStr = attrVal;
                                for(int i = 0; i< countries.length;i++) {
                                    if (locationStr.contains(countries[i])) {
                                        location = new Text(countries[i]);
                                        break;
                                    }
                                }
                                break;
                            case "UpVotes":
                                upvotes = Integer.parseInt(attrVal);
                            case "Reputation":
                                reputation = Integer.parseInt(attrVal);
                                break;
                            case "Views":
                                views = Integer.parseInt(attrVal);
                                break;
                            case "CreationDate":
                                creationDate = new Text(attrVal.substring(0,10));
                                break;
                            default:
                                break;
                        }
                    }

                }

                if(location!= null) {

                    // Creating a list of reputed users, to be pair with location below
                    List<RepUserWritable> userList = new ArrayList<>();

                    // Creating user writable containing meta-data for users
                    RepUserWritable hW = new RepUserWritable(displayName,website,upvotes,views,creationDate,reputation);
                    userList.add(hW);

                    // Creating location writable to store location-wise users
                    LocationWritable wW = new LocationWritable(1, location, userList);
                    context.write(location, wW);
                }
            }
        }
        catch (Exception e){
//            logger.error("HighestRepUserMapper : "+ e.getMessage());
            e.printStackTrace();
        }

    }

}
