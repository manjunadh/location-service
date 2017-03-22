package com.fgl.repository;


import com.fgl.domain.Store;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StoreRepository extends CrudRepository<Store, Long> {

    List<Store> findByProvince(@Param("province") String province);
    List<Store> findByPointNear(@Param("location") Point location, @Param("distance") Distance distance);







}
