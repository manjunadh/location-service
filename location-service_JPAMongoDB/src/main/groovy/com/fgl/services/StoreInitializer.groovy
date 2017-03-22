package com.fgl.services

import com.fgl.domain.Store
import com.fgl.repository.StoreRepository
import groovy.json.JsonSlurper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.geo.Point
import org.springframework.stereotype.Component

/**
 * Created by rhett on 2017-03-18.
 */
@Component
class StoreInitializer {


    @Autowired
    public StoreInitializer(StoreRepository repository) throws Exception {

        if (repository.count() != 0) {
            return;
        }

        List<Store> stores = readStores();
        println "######## Importing ${stores.size()} stores into DB #########"
        repository.save(stores);

    }

    List<Store> readStores() {

        // json file exported from DBVisualizer as JSON
        def jsonFile = new File(getClass().getResource('/fgl_stores.json').toURI())
        def jsonSlurper = new JsonSlurper()
        def storesJSON = jsonSlurper.parse(jsonFile)

        def stores = []

        storesJSON.each{

            def point

            if (it.LONGITUDE && it.LATITUDE)
                point = new Point(new Double(it.LONGITUDE),new Double(it.LATITUDE))


            def store = new Store(
                    storeNumber: it.ORG_LVL_NUMBER,
                    name:it.ORG_NAME_FULL,
                    address:"${it.BAS_ADDR_1} ${it.BAS_ADDR_2}",
                    city:it.BAS_CITY,
                    province:it.BAS_STATE,
                    longitude:it.LONGITUDE,
                    latitude:it.LATITUDE,
                    point: point
            )

            // add it to the list
            stores << store

        }

        stores

    }
}
