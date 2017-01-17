package serving;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by tom on 17.01.17.
 */
@Table(keyspace = "tutorialspoint", name = "mean_arrival")
public class AirportStats {

    @Column(name = "source")
    String source;
    @Column(name = "dest")
    String dest;
    @Column(name = "number_of_flights")
    Long number;
    @Column(name = "sum_of_delays")
    Long sum;
    public AirportStats(){}
}