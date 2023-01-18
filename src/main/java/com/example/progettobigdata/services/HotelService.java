package com.example.progettobigdata.services;
import com.example.progettobigdata.dto.HotelNationalityResult;
import com.example.progettobigdata.dto.NegativeReviewsPerHotel;
import com.example.progettobigdata.dto.Percentuale;
import com.example.progettobigdata.repositories.HotelRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;


@Service
public class HotelService {

    @Autowired
    HotelRepository repository;

     public List<NegativeReviewsPerHotel> queryProva(int offset, int limit){


        try {
            Dataset<Row> data = this.repository.queryDiProva();
            List<NegativeReviewsPerHotel> ret =  data.collectAsList().stream().map(riga -> NegativeReviewsPerHotel.convertFromRow(riga)).collect(Collectors.toList());
            int pageSize = limit;
            int start = pageSize * offset;
            //if(pageSize+limit > ret.size() ) return new List();
            // - - - - - - -
            //               _  _
            //             *

            return ret.subList(start, start+limit);

        }catch( IOException e){
            throw new RuntimeException(e);
        }
    }

    public List<Percentuale> Negative() {
        try {
            return this.repository.getNegative();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    public HashMap<String, Double> Nazionalità(String hotelName){
         try {
              List<HotelNationalityResult> res = this.repository.getNazionalità(hotelName);
              long totale = this.repository.getTotale(hotelName);
              HashMap<String, Double> risultato = new HashMap<String, Double>();
              double percentuale;
              for(HotelNationalityResult hot : res){
                    percentuale = (double) hot.getReviewer_Count()/totale*100;
                    risultato.put(hot.getReviewer_Nationality(),percentuale);
              }
              return risultato;
         }catch(IOException e){
             throw new RuntimeException(e);
         }
    }



    /*public List<RowDTO> getByHotel(String nome, int limit)  {
        try{

            Dataset<Row> data = this.repository.getByName(nome,limit);
            data.show();
            // stream -> introdotto con le lambda ed è consigliato per le liste grandi
            // map -> fa svolgere operazioni sui singoli elementa dalla lista
            // collect -> si usa sugli array/dataset/hashmap e serve per convertire la lista nel tipo di lista voluta
            return data.collectAsList().stream().map( r-> RowDTO.convertFromRow(r)).collect(Collectors.toList());

        } catch( IOException e) {
            throw new RuntimeException(e);
        }
    }*/

}
