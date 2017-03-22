package com.fgl.domain

import org.springframework.data.mongodb.core.index.GeoSpatialIndexed
import org.springframework.data.mongodb.core.mapping.Document
import static org.springframework.data.mongodb.core.index.GeoSpatialIndexType.*;


import javax.persistence.Column
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;

/**
 * Created by rhett on 2017-03-16.
 */
@Document
class Store {

    @Id
    String storeNumber
    String name
    String address
    String city
    String province
    String longitude
    String latitude

    @GeoSpatialIndexed(type = GEO_2DSPHERE) Point location;


}
