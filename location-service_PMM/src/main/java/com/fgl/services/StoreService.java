package com.fgl.services;

import com.fgl.domain.DistanceResult;
import com.fgl.domain.Store;

import java.util.List;

/**
 * Created by rhett on 2017-03-16.
 */
public interface StoreService {


    public Store getStore(String storeNumber);
    public List<Store> getAllStores();
    public List<Store> getAllStoresForProvince(String province);
    public List<DistanceResult> getAllStoresNearby(String storeNumber);

}
