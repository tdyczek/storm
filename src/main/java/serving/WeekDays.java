package serving;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "tutorialspoint", name = "days_of_week")
public class WeekDays {

    @Column(name = "day")
    int day;
    @Column(name = "no_of_punctual")
    Long no_of_punctual;
    public WeekDays(){}
}