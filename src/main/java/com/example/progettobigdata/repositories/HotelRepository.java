package com.example.progettobigdata.repositories;
import com.example.progettobigdata.dto.AverageWordCounter;
import com.example.progettobigdata.dto.HotelNationalityResult;
import com.example.progettobigdata.dto.Percentuale;
import com.example.progettobigdata.dto.SeasonalReviewCounterDTO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;


@Repository
public class HotelRepository {

    private SparkSession spark = null;
    private Dataset<Row> dataset = null;
    private enum season {SUMMER, WINTER, SPRING, AUTUMN};

    private Dataset<Row> readDataset() throws IOException {

        if(spark == null) this.initSpark();
        Resource resource = new ClassPathResource("Hotel_Reviews.csv");
        String datasetPath = resource.getFile().getPath();

        Dataset<Row> data = this.spark.read().option("header", "true").
                option("inferSchema", "true").csv(datasetPath);
        return data;
    }

    @PostConstruct
    public void init() throws IOException {
        this.dataset = this.readDataset();
        this.dataset.createOrReplaceTempView("recensioni");
    }


    private void initSpark() {
        this.spark = SparkSession.builder().appName("Spark Test for Big Data")
                .config("spark.master", "local").getOrCreate();
    }


    //restituisce il totale delle recensioni di un determinato Hotel passato come parametro
    public long getTotale(String hotelName){
        Dataset<Row> row = this.spark.sql("SELECT COUNT(*) FROM recensioni WHERE Hotel_Name=\""+hotelName+"\"");
        return row.collectAsList().stream().map(r -> r.getLong(0)).collect(Collectors.toList()).get(0);
    }


    /*restituisce, per un hotel passato come parametro, la percentuale di inglesi, francesi ecc.
     che hanno lasciato una recensione*/
    public List<HotelNationalityResult> getNazionalità(String nomeHotel) throws IOException {

        Dataset<Row> datarow_Nazionalita = this.spark.sql("SELECT Reviewer_Nationality, " +
                            "COUNT(*) AS Reviewers_Number " +
                            "FROM recensioni " +
                            "WHERE Hotel_Name=\"" + nomeHotel + "\""+
                            "GROUP BY Reviewer_Nationality");

        return datarow_Nazionalita.collectAsList().stream()
                            .map(r -> HotelNationalityResult.convertFromRow(r)).collect(Collectors.toList());
    }//getNazionalità


    /*Ritorna, dato il nome di un hotel, la percentuale di recensioni Negative
    (tra 0 e 4) Positive (>6) e neutre(tra 4 e 6)*/
    public List<Percentuale> getPercentuals(String hotelName) throws IOException {

        String totNeutre = "SELECT  COUNT(*) as TOT FROM recensioni " +
                " WHERE Hotel_Name = \""+hotelName+"\" AND Reviewer_Score >= 4 AND Reviewer_Score < 6  ";
        Dataset<Row> hotelNeutre= this.spark.sql(totNeutre);
        hotelNeutre.createOrReplaceTempView("hotelNeutre");

        String totNeg = "SELECT COUNT(*) as TOT FROM recensioni " +
                " WHERE Reviewer_Score < 4 AND Hotel_Name = \""+hotelName+"\"";
        Dataset<Row> hotelNeg= this.spark.sql(totNeg);
        hotelNeg.createOrReplaceTempView("hotelNeg");

        String totPos = "SELECT COUNT(*) as TOT FROM recensioni " +
                " WHERE Reviewer_Score >=6 AND Hotel_Name = \""+hotelName+"\"";
        Dataset<Row> hotelPos= this.spark.sql(totPos);
        hotelPos.createOrReplaceTempView("hotelPos");

        String percentuali = "SELECT  hotelNeg.TOT as Totale_Negative, " +
                "             hotelPos.TOT as Totale_Positive, hotelNeutre.TOT as Totale_Neutre " +
                "FROM hotelNeg, hotelPos, hotelNeutre ";
        Dataset<Row> percento = this.spark.sql(percentuali);
        List<Percentuale> ret =
                percento.collectAsList().stream().map(r->Percentuale.convertFromRow(r)).collect(Collectors.toList());
        return ret;
    }

    //Restituisce per ogni nazionalità, la media di parole per recensione
    public List<AverageWordCounter> getMostAccurate() throws IOException {
        Dataset<Row> totaleRece = this.spark.sql(
                "SELECT Reviewer_Nationality," +
                        "AVG(Review_Total_Positive_Word_Counts + Review_Total_Negative_Word_Counts) as Media " +
                "FROM recensioni " +
                "GROUP BY Reviewer_Nationality ORDER BY Media DESC ");
        List<AverageWordCounter> awc =
                totaleRece.collectAsList().stream().map(r->AverageWordCounter.convertFromRow(r)).collect(Collectors.toList());
        return awc;

    }

    //seleziono tutti i tag dal dataset (mi serve per separarli)
    public Dataset<Row> getTags() {
           Dataset<Row> totaleTags = this.spark.sql("SELECT Tags " +
                   "FROM recensioni");
           return totaleTags;
    }

    /*seleziono solo i campi Hotel_Name e Tags dal dataset, per fare poi una ricerca
    su quali hotel compaiono determinati tag*/
    public Dataset<Row> getNameTags() {
           Dataset<Row> totaleNameTags = this.spark.sql("SELECT Hotel_Name, Tags " +
                   "FROM recensioni");
           return totaleNameTags;
    }




    /*Crea una data a partire da una stringa e poi identifica la stagione restituendola come stringa*/
    private String identifySeason(String d){
        StringTokenizer st = new StringTokenizer(d,"/");
        int mese = Integer.parseInt(st.nextToken());
        int giorno = Integer.parseInt(st.nextToken());
        int anno = Integer.parseInt(st.nextToken());
        GregorianCalendar data = new GregorianCalendar(anno,mese,giorno);
        GregorianCalendar springStart = new GregorianCalendar(anno, 2, 20); //20 marzo
        GregorianCalendar springEnd = new GregorianCalendar(anno, 5, 21); //21 giugno
        GregorianCalendar summerStart = new GregorianCalendar(anno, 5, 21); //21 giugno
        GregorianCalendar summerEnd = new GregorianCalendar(anno, 8, 23); //23 settembre
        GregorianCalendar autumnStart = new GregorianCalendar(anno,8,23); //23 settembre
        GregorianCalendar autumnEnd = new GregorianCalendar(anno,11,21); //21 dicembre
      //  GregorianCalendar winterStart = new GregorianCalendar(anno, 11, 21); //22 dicembre
      //  GregorianCalendar winterEnd = new GregorianCalendar(anno, 2, 20); //20 marzo
        if(data.after(springStart) && data.before(springEnd))
            return "Primavera";
        if(data.after(summerStart) && data.before(summerEnd))
            return "Estate";
        if(data.after(autumnStart) && data.before(autumnEnd))
            return "Autunno";
        return "Inverno";
    }


    /*Dato un Hotel passato come parametro, restituisco il numero di recensioni lasciate per ogni stagione*/
    public SeasonalReviewCounterDTO getData(String hotelName) {
        Dataset<Row> data = this.spark.sql("SELECT Review_Date FROM recensioni WHERE Hotel_Name = \"" + hotelName + "\"");
        Dataset<Row> totRece = this.spark.sql("SELECT COUNT(*) FROM recensioni WHERE Hotel_Name = \"" + hotelName + "\"");
        totRece.show();
        List<String> date = data.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
        SeasonalReviewCounterDTO season = new SeasonalReviewCounterDTO();  //0 spring, 1 summer, 2 autumn, 3 winter
        for(String d : date) {
                season.incSeason(identifySeason(d));
        }//for

        return season;
    }

}
