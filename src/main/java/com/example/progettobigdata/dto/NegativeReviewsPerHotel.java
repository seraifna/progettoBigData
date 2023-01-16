package com.example.progettobigdata.dto;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.sql.Row;

import java.io.Serializable;

@Data
@AllArgsConstructor

public class NegativeReviewsPerHotel implements Serializable {
    private String hotelName;
    private long count;

    public static NegativeReviewsPerHotel convertFromRow(Row row){
        return new NegativeReviewsPerHotel(row.getString(0), row.getLong(1));
    }

}
