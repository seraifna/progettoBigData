package com.example.progettobigdata.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.sql.Row;

/*Dato il nome di un  hotel come parametro, mi restituisce la percentuale di italiani, inglesi */
@AllArgsConstructor
@Data
public class HotelNationalityResult {

    private String reviewer_Nationality;
    private long reviewer_Count;
    public static HotelNationalityResult convertFromRow (Row r){
        return new HotelNationalityResult(r.getString(0), r.getLong(1));
    }

}
