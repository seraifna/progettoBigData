package com.example.progettobigdata.repositories;
import com.example.progettobigdata.dto.Percentuale;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

@Repository
public class HotelRepository {


    private SparkSession spark = null;
    private Dataset<Row> dataset = null;

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


    public Dataset<Row> queryDiProva() throws IOException {
//        Dataset<Row> ret = this.readDataset();
//        ret.createOrReplaceTempView("recensioni");

        String innerQuery = " SELECT Hotel_Name, COUNT(*) FROM recensioni " +
        "WHERE recensioni.Review_Total_Negative_Word_Counts > recensioni.Review_Total_Positive_Word_Counts " +
                "GROUP BY Hotel_Name ";
        //******    QUERY TOTAL NEGATIVE REVIEW     ******//
        return this.spark.sql( innerQuery );

        /*  Dataset<Row> count_tuple = ret.select(col("Hotel_Name"));
        count_tuple.show();
        long conta = count_tuple.dropDuplicates().count();
        System.out.println("numero totale di hotel:"+ conta);*/
    }

    private void initSpark() {
        this.spark = SparkSession.builder().appName("Spark Test for Big Data")
                .config("spark.master", "local").getOrCreate();
    }


    public List<Percentuale> getNegative() throws IOException {


//        Dataset<Row> totRece = this.spark.sql("SELECT Hotel_Name, COUNT(*) as TOT FROM recensioni GROUP BY Hotel_Name");
//        totRece.createOrReplaceTempView("totRece");


        String totNeutre = "SELECT Hotel_Name, COUNT(*) as TOT FROM recensioni " +
                " WHERE recensioni.Reviewer_Score >= 4 AND recensioni.Reviewer_Score < 6  "+
                "GROUP BY Hotel_Name";
        Dataset<Row> hotelNeutre= this.spark.sql(totNeutre);
        hotelNeutre.createOrReplaceTempView("hotelNeutre");

        String totNeg = "SELECT Hotel_Name, COUNT(*) as TOT FROM recensioni " +
                " WHERE recensioni.Reviewer_Score < 4 "+
                "GROUP BY Hotel_Name";
        Dataset<Row> hotelNeg= this.spark.sql(totNeg);
        hotelNeg.createOrReplaceTempView("hotelNeg");

        //Dataset<Row> : nomeHotel , percentualePos
        String totPos = "SELECT Hotel_Name, COUNT(*) as TOT FROM recensioni " +
                " WHERE recensioni.Reviewer_Score >=6 " +
                "GROUP BY Hotel_Name";
        Dataset<Row> hotelPos= this.spark.sql(totPos);
        hotelPos.createOrReplaceTempView("hotelPos");


        String percentuali = "SELECT hotelNeg.Hotel_Name,  hotelNeg.TOT as Totale_Negative, " +
                "             hotelPos.TOT as Totale_Positive, hotelNeutre.TOT as Totale_Neutre " +
                "FROM hotelNeg, hotelPos, hotelNeutre " +
                "WHERE hotelNeg.Hotel_Name = hotelPos.Hotel_Name AND hotelNeg.Hotel_Name = hotelNeutre.Hotel_Name";
        Dataset<Row> percento = this.spark.sql(percentuali);


        List<Percentuale> ret = percento.collectAsList().stream().map(r-> Percentuale.convertFromRow(r)).collect(Collectors.toList());
        return ret;
    }

}