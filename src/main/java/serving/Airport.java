package serving;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by tom on 17.01.17.
 */
@Table(keyspace = "tutorialspoint", name = "airports")
public class Airport {

    @Column(name = "airport_name")
    String airportName;
    @Column(name = "number")
    Long number;
    public Airport(){}
}