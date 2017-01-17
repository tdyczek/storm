package serving;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by tom on 17.01.17.
 */



public class Accum {

    public static void main(String[] args){
        Cluster cluster = null;
        Session session = null;
        cluster = Cluster.builder()                                                    // (1)
                .addContactPoint("127.0.0.1")
                .build();
        session = cluster.connect();
        MappingManager manager = new MappingManager(session);

// zad1
//        Mapper<WeekDays> mapper = manager.mapper(WeekDays.class);
//        ResultSet results = session.execute("SELECT * FROM tutorialspoint.days_of_week");
//        List<WeekDays> users = mapper.map(results).all();
//        users.sort(new Comparator<WeekDays>() {
//            public int compare(WeekDays o1, WeekDays o2) {
//                if(o1.no_of_punctual >= o2.no_of_punctual)
//                    return -1;
//                return 1;
//            }
//        });
//        for (WeekDays w : users) {
//            System.out.println(Integer.toString(w.day) + " " + Long.toString(w.no_of_punctual));
//        }


//        zad2
//        Mapper<Airport> mapper = manager.mapper(Airport.class);
//        ResultSet results = session.execute("SELECT * FROM tutorialspoint.airports");
//        List<Airport> airports = mapper.map(results).all();
//        airports.sort(new Comparator<Airport>() {
//            public int compare(Airport o1, Airport o2) {
//                if(o1.number >= o2.number)
//                    return -1;
//                return 1;
//            }
//        });
//        for (Airport w : airports) {
//            System.out.println(w.airportName + " " + Long.toString(w.number));
//        }

    //zad3
//    String airport = "SAT";
//    Mapper<AirportCarrier> mapper = manager.mapper(AirportCarrier.class);
//    ResultSet results = session.execute("SELECT * FROM tutorialspoint.popular_airports");
//    List<AirportCarrier>  airportCarriers = mapper.map(results).all();
//    List<AirportCarrier> filtered = new ArrayList<AirportCarrier>();
//    for(AirportCarrier ac : airportCarriers)
//        if(ac.airportName.equals(airport)){
//            filtered.add(ac);
//        }
//     airportCarriers = filtered;
//        airportCarriers.sort(new Comparator<AirportCarrier>() {
//            public int compare(AirportCarrier o1, AirportCarrier o2) {
//                if(o1.number >= o2.number)
//                    return -1;
//                return 1;
//            }
//        });
//        for (AirportCarrier w : airportCarriers) {
//            System.out.println(w.carrier + " " + Long.toString(w.number));
//        }

//        zad4
//        Mapper<AirportStats> mapper = manager.mapper(AirportStats.class);
//        ResultSet results = session.execute("SELECT * FROM tutorialspoint.mean_arrival");
//        List<AirportStats> airports = mapper.map(results).all();
//        airports.sort(new Comparator<AirportStats>() {
//            public int compare(AirportStats o1, AirportStats o2) {
//                if(o1.sum/o1.number >= o2.sum/o2.number)
//                    return 1;
//                else return  -1;
//            }
//        });
//
//        for (AirportStats w : airports) {
//            System.out.println(w.source+ " " + w.dest + " " + Float.toString(w.sum/w.number));
//        }


    }
}
