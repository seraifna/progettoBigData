package com.example.progettobigdata.dto;
import org.apache.spark.sql.Row;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
@Data
@AllArgsConstructor
public class NameTags implements Serializable {

    private String  hotelName, tag;

    public static NameTags convertFromRow(Row r){
        String hotelName = r.getString(0);
        String tag = r.getString(1);
        return new NameTags(hotelName,tag);
    }


}
