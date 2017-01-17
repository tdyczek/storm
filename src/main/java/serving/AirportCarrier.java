package serving;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by tom on 17.01.17.
 */
@Table(keyspace = "tutorialspoint", name = "popular_airports")
public class AirportCarrier {

    @Column(name = "source")
    String airportName;
    @Column(name = "carrier")
    String carrier;
    @Column(name = "number_of_flights")
    Long number;
    public AirportCarrier(){}
}