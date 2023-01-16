package com.example.progettobigdata.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.sql.Row;

import java.io.Serializable;

@Data
@AllArgsConstructor

public class Percentuale implements Serializable {

    private String nomeHotel;
    private long totRece, totN, totP, totNeutre;
    private double percentualeNeg;
    private double percentualePos;
    private double percentualeNeutre;

    public static Percentuale convertFromRow(Row r) {
        String nomeHotel = r.getString(0);
        long totN = r.getLong(1);
        long totP = r.getLong(2);
        long totNeutre= r.getLong(3);
        long totRece = totN + totP + totNeutre;
        double percentualeNeg =  Math.round(((double)totN/totRece)*100);
        double percentualePos =  Math.round(((double)totP/totRece)*100);
        double percentualeNeutre = Math.round(((double)totNeutre/totRece)*100);
        return new Percentuale(nomeHotel, totRece, totN, totP, totNeutre, percentualeNeg, percentualePos, percentualeNeutre);
    }//convertFromRow

}
