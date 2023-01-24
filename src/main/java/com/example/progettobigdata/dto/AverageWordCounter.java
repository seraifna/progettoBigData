package com.example.progettobigdata.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.sql.Row;

@Data
@AllArgsConstructor
public class AverageWordCounter implements Serializable{

    private String Nationality;
    private double media;

    public static AverageWordCounter convertFromRow(Row r){
        String nationality = r.getString(0);
        double media = r.getDouble(1);
        return new AverageWordCounter(nationality, media);
    }

}
