package com.example.progettobigdata.services;
import com.example.progettobigdata.dto.*;
import com.example.progettobigdata.repositories.HotelRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


@Service
public class HotelService {

    @Autowired
    HotelRepository repository;

//

    public List<Percentuale> Percentuals(String hotelName) {
        try {
            return this.repository.getPercentuals(hotelName);
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

    public List<AverageWordCounter> Accurate() {
         try{
             List<AverageWordCounter> ret = this.repository.getMostAccurate();
             return ret;
         }catch (IOException e) {
             throw new RuntimeException(e);
         }
    }

    //mi prendo un hashset degli hotel con recensioni in cui compaiono i tag che passo come lista di stringhe
    public HashSet<String> getByTags(List<String> tags){
         HashSet<String> ret = new HashSet<String>();
        try {
            Dataset<Row> nameTags = this.repository.getNameTags();
            List<NameTags> lista= nameTags.collectAsList().stream().
                    map(r -> NameTags.convertFromRow(r)).collect(Collectors.toList());
            for(NameTags hotel : lista){
                if(!ret.contains(hotel.getHotelName())){
                    boolean toAdd = true;
                    inner: for(String tag : tags){
                                if (!hotel.getTag().contains(tag)) {
                                    toAdd = false;
                                    break inner;
                                }
                            }//innerfor
                    if(toAdd) ret.add(hotel.getHotelName());
                }
            }//for esterno
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    //mi prende i tag separati da , e [] e '
    private ArrayList<String> parseTagArray(String toParse){
        StringTokenizer tk = new StringTokenizer(toParse, "[],''");
        ArrayList<String> ret = new ArrayList<>(10);
        while(tk.hasMoreTokens()){
            String tmp = (String) tk.nextToken();
        //    System.out.println(tmp);
            tmp=tmp.trim();
            if (tmp.equals("")) continue;
            else ret.add(new String (tmp));
        }
        return ret;
    }


    //metodo per restituirmi tutti i tag presi una volta per ognuno
    public HashSet<String> Tags(){
         Dataset<Row> tagsRisultato = this.repository.getTags();
         List<String> arrayTags = tagsRisultato.collectAsList().stream().map(r -> r.getString(0)).collect(Collectors.toList());
         ArrayList<String> tags = new ArrayList<>(100);
         for(String tag : arrayTags) {
             tags.addAll(parseTagArray(tag));
         }
         return new HashSet<String>(tags);
    }



    public SeasonalReviewCounterDTO getSeason(String hotelName) {
        try {
            SeasonalReviewCounterDTO date = this.repository.getData(hotelName);
            return date;
        }catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

}
