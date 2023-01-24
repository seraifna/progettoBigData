package com.example.progettobigdata.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.sql.Row;

import java.io.Serializable;

@Data
@AllArgsConstructor

public class Percentuale implements Serializable {

    private double percentualeNeg;
    private double percentualePos;
    private double percentualeNeutre;

    public static Percentuale convertFromRow(Row r) {
        long totN = r.getLong(0);
        long totP = r.getLong(1);
        long totNeutre= r.getLong(2);
        long totRece = totN + totP + totNeutre;
        double percentualeNeg = ((double) totN/totRece)*100;
        double percentualePos =  ((double) totP/totRece)*100;
        double percentualeNeutre = ((double) totNeutre/totRece)*100;
        return new Percentuale(percentualeNeg, percentualePos, percentualeNeutre);
    }//convertFromRow

}
