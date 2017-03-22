package com.fgl.services

import com.fgl.domain.Store
import com.fgl.repository.StoreRepository
import groovy.sql.Sql
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.geo.Distance
import org.springframework.data.geo.Metrics
import org.springframework.data.geo.Point
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Service

import javax.sql.DataSource

/**
 * Created by rhett on 2017-03-15.
 */
@Service("groovyStoreService")
class StoreServiceGroovySQLImpl implements StoreService {

    private static final Distance DEFAULT_DISTANCE = new Distance(50, Metrics.KILOMETERS);



    @Autowired
    DataSource dataSource;

    @Autowired
    StoreRepository storeRepository;


    @Override
    Store getStore(String storeNumber) {
        Store newStore = storeRepository.findOne(storeNumber)
        return newStore
    }

    @Override
    List<Store> getAllStores() {
    def stores = storeRepository.findAll()
        stores
    }

    @Override
    List<Store> getAllStoresForProvince(String province) {

        def stores = storeRepository.findByProvince(province)

        stores
    }

    @Override
    List<Store> getAllStoresNearby(String storeNumber) {

        Store store = storeRepository.findOne(storeNumber)

        // TODO will not work with embedded H2
        def nearByStores = storeRepository.findByPointNear(store.point,DEFAULT_DISTANCE)


        return storeRepository.findAll()
    }
}
