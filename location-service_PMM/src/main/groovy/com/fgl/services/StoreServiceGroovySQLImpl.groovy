package com.fgl.services

import com.fgl.domain.DistanceResult
import com.fgl.domain.Store
import groovy.sql.Sql
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import javax.sql.DataSource

/**
 * Created by rhett on 2017-03-15.
 */
@Service("groovyStoreService")
class StoreServiceGroovySQLImpl implements StoreService {

    // do SQL the Groovy way!

    @Autowired
    DataSource dataSource;

    static def cachedStores

    SpatialSearch spatialSearch

    int defaultDistance = 10

    @Override
    Store getStore(String storeNumber) {

        Sql connection = new Sql(dataSource)

        // first attempt at SQL - still need to add long/lat
        def sql = """
            SELECT    
            mst.ORG_LVL_CHILD  
            , mst.ORG_LVL_NUMBER  
            , mst.ORG_NAME_SHORT 
            , mst.ORG_NAME_FULL  
            , dtl.ORG_DATE_OPENED  
            , dtl.ORG_DATE_CLOSED 
            , dtl.ORG_LVL_ACTIVE
            , TRIM(adr.BAS_ADDR_1) AS BAS_ADDR_1 
            , TRIM(adr.BAS_ADDR_2) AS BAS_ADDR_2  
            , TRIM(adr.BAS_ADDR_3) AS BAS_ADDR_3  
            , TRIM(adr.BAS_CITY) AS BAS_CITY  
            , TRIM(adr.BAS_STATE) AS BAS_STATE  
            , TRIM(adr.BAS_ZIP) AS BAS_ZIP  
            , TRIM(ctry.CNTRY_CODE) AS CNTRY_CODE  
            , TRIM(adr.BAS_ISD_FAX) AS BAS_ISD_FAX  
            , TRIM(adr.BAS_ISD) AS BAS_ISD  
            , TRIM(adr.BAS_AREA) AS BAS_AREA 
            , TRIM(adr.BAS_AREA_FAX) AS BAS_AREA_FAX  
            , TRIM(adr.BAS_PHONE_NUMB) AS BAS_PHONE_NUMB  
            , TRIM(adr.BAS_FAX_NUMBER) AS BAS_FAX_NUMBER 
            , TRIM(adr.VPC_EMAIL) AS VPC_EMAIL  
            , dtl.ORG_MANAGER_NAME  
            , val.VALUE AS WMSWHSE
            , mst.ORG_IS_STORE  
            ,basa.ATR_CODE_DESC STORE_TYPE
            ,lon.VALUE as LONGITUDE
            ,lat.VALUE as LATITUDE
            ,sq_ftg.VALUE as SQUARE_FOOTAGE
            ,CASE to_char(tz.VALUE)
              WHEN '10' THEN 'PST'
              WHEN '15' THEN 'MST'
              WHEN '20' THEN 'CST'
              WHEN '25' THEN 'EST'
              WHEN '30' THEN 'AST'
              WHEN '35' THEN 'NST' 
              END AS TIMEZONE
            ,dlt.VALUE as DAYLIGHT_SAVING
            ,basa1.ATR_CODE STORE_STATUS_CODE
            ,basa1.ATR_CODE_DESC STORE_STATUS
            ,typ.ORG_TYPE_CODE as ORG_TYPE_CODE
            ,typ.ORG_TYPE_DESC AS ORG_TYPE_DESC
            ,mst.CURR_CODE CURR_CODE
            FROM INT_FGL_PMM_RPL.ORGMSTEE mst
            INNER JOIN INT_FGL_PMM_RPL.ORGDTLEE dtl ON mst.ORG_LVL_CHILD = dtl.ORG_LVL_CHILD
            LEFT JOIN INT_FGL_PMM_RPL.ORGTYPEE typ ON typ.ORG_TYPE_CODE=dtl.ORG_TYPE_CODE
            --LEFT JOIN BASATOEE ato ON mst.ORG_LVL_CHILD=ato.ORG_LVL_CHILD
            --LEFT JOIN BASAHREE ahr ON ahr.ATR_HEADER_DESC='Store Type' AND ahr.ATR_HDR_TECH_KEY=ato.ATR_HDR_TECH_KEY
            --LEFT JOIN BASACDEE acd ON acd.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY
            left join 
            (SELECT ato.ORG_LVL_CHILD, ahr.ATR_HEADER_DESC, acd.ATR_CODE, acd.ATR_CODE_DESC
                        FROM INT_FGL_PMM_RPL.BASATOEE ato
                        INNER JOIN INT_FGL_PMM_RPL.BASATYEE aty ON ato.atr_typ_tech_key = aty.atr_typ_tech_key
                        INNER JOIN INT_FGL_PMM_RPL.BASAHREE ahr ON ato.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY
                        INNER JOIN INT_FGL_PMM_RPL.BASACDEE acd ON acd.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY AND acd.ATR_COD_TECH_KEY=ato.ATR_COD_TECH_KEY
                        WHERE ahr.ATR_HEADER_DESC = 'Store Type'
                              and aty.atr_type_desc = 'Store Type'
                              and ahr.app_func = 'ORG'
                              and ahr.app_name = 'BAS')basa
            on mst.org_lvl_child=basa.org_lvl_child
            left join 
            (SELECT ato.ORG_LVL_CHILD, ahr.ATR_HEADER_DESC, acd.ATR_CODE, acd.ATR_CODE_DESC
                        FROM INT_FGL_PMM_RPL.BASATOEE ato
                        INNER JOIN INT_FGL_PMM_RPL.BASATYEE aty ON ato.atr_typ_tech_key = aty.atr_typ_tech_key
                        INNER JOIN INT_FGL_PMM_RPL.BASAHREE ahr ON ato.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY
                        INNER JOIN INT_FGL_PMM_RPL.BASACDEE acd ON acd.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY AND acd.ATR_COD_TECH_KEY=ato.ATR_COD_TECH_KEY
                        WHERE ahr.ATR_HEADER_DESC = 'Store Status'
                              and aty.atr_type_desc = 'Store Status'
                              and ahr.app_func = 'ORG'
                              and ahr.app_name = 'BAS')basa1
            on mst.org_lvl_child=basa1.org_lvl_child
            LEFT JOIN INT_FGL_PMM_RPL.BASCOOEE ctry ON dtl.CNTRY_LVL_CHILD = ctry.CNTRY_LVL_CHILD
            LEFT JOIN INT_FGL_PMM_RPL.BASADREE adr ON  dtl.BAS_ADD_KEY = adr.BAS_ADD_KEY
            LEFT JOIN INT_FGL_PMM_RPL.BASVALEE val ON val.ENTITY_NAME = 'ORGMSTEE' AND val.TECH_KEY1 = mst.ORG_LVL_CHILD AND val.FIELD_CODE = '15' -- WMSWHSE
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                       ) lon 
                          ON lon.entity_name = 'ORGMSTEE' 
                             AND lon.tech_key1 = mst.org_lvl_child 
                             AND lon.field_code = '45' -- Longitude 
                   left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                  ) lat 
                          ON lat.entity_name = 'ORGMSTEE' 
                             AND lat.tech_key1 = mst.org_lvl_child 
                             AND lat.field_code = '40' -- Latitude 
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                       ) sq_ftg 
                          ON sq_ftg .entity_name = 'ORGMSTEE' 
                             AND sq_ftg .tech_key1 = mst.org_lvl_child 
                             AND sq_ftg .field_code = '20' -- Square Footage
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                  ) tz
                          ON tz.entity_name = 'ORGMSTEE' 
                             AND tz.tech_key1 = mst.org_lvl_child 
                             AND tz.field_code = '63' -- Time Zone
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                  ) dlt
                          ON dlt.entity_name = 'ORGMSTEE' 
                             AND dlt.tech_key1 = mst.org_lvl_child 
                             AND dlt.field_code = '64' -- Day Light Saving flag
            WHERE mst.ORG_LVL_ID = 1
            and mst.ORG_LVL_NUMBER=${storeNumber}
        """


        def result = connection.rows(sql)

        Store store = new Store()
        store.name = result[0].ORG_NAME_FULL
        store.storeNumber = storeNumber
        store.address = result[0].BAS_ADDR_1 + " " + result[0].BAS_ADDR_2
        store.city = result[0].BAS_CITY


        store.longitude = Double.parseDouble(result[0].LONGITUDE)
        store.latitude = Double.parseDouble(result[0].LATITUDE)

        return store
    }

    @Override
    List<Store> getAllStores() {

        Sql connection = new Sql(dataSource)


        def sql = """
            SELECT    
            mst.ORG_LVL_CHILD  
            , mst.ORG_LVL_NUMBER  
            , mst.ORG_NAME_SHORT 
            , mst.ORG_NAME_FULL  
            , dtl.ORG_DATE_OPENED  
            , dtl.ORG_DATE_CLOSED 
            , dtl.ORG_LVL_ACTIVE
            , TRIM(adr.BAS_ADDR_1) AS BAS_ADDR_1 
            , TRIM(adr.BAS_ADDR_2) AS BAS_ADDR_2  
            , TRIM(adr.BAS_ADDR_3) AS BAS_ADDR_3  
            , TRIM(adr.BAS_CITY) AS BAS_CITY  
            , TRIM(adr.BAS_STATE) AS BAS_STATE  
            , TRIM(adr.BAS_ZIP) AS BAS_ZIP  
            , TRIM(ctry.CNTRY_CODE) AS CNTRY_CODE  
            , TRIM(adr.BAS_ISD_FAX) AS BAS_ISD_FAX  
            , TRIM(adr.BAS_ISD) AS BAS_ISD  
            , TRIM(adr.BAS_AREA) AS BAS_AREA 
            , TRIM(adr.BAS_AREA_FAX) AS BAS_AREA_FAX  
            , TRIM(adr.BAS_PHONE_NUMB) AS BAS_PHONE_NUMB  
            , TRIM(adr.BAS_FAX_NUMBER) AS BAS_FAX_NUMBER 
            , TRIM(adr.VPC_EMAIL) AS VPC_EMAIL  
            , dtl.ORG_MANAGER_NAME  
            , val.VALUE AS WMSWHSE
            , mst.ORG_IS_STORE  
            ,basa.ATR_CODE_DESC STORE_TYPE
            ,lon.VALUE as LONGITUDE
            ,lat.VALUE as LATITUDE
            ,sq_ftg.VALUE as SQUARE_FOOTAGE
            ,CASE to_char(tz.VALUE)
              WHEN '10' THEN 'PST'
              WHEN '15' THEN 'MST'
              WHEN '20' THEN 'CST'
              WHEN '25' THEN 'EST'
              WHEN '30' THEN 'AST'
              WHEN '35' THEN 'NST' 
              END AS TIMEZONE
            ,dlt.VALUE as DAYLIGHT_SAVING
            ,basa1.ATR_CODE STORE_STATUS_CODE
            ,basa1.ATR_CODE_DESC STORE_STATUS
            ,typ.ORG_TYPE_CODE as ORG_TYPE_CODE
            ,typ.ORG_TYPE_DESC AS ORG_TYPE_DESC
            ,mst.CURR_CODE CURR_CODE
            FROM INT_FGL_PMM_RPL.ORGMSTEE mst
            INNER JOIN INT_FGL_PMM_RPL.ORGDTLEE dtl ON mst.ORG_LVL_CHILD = dtl.ORG_LVL_CHILD
            LEFT JOIN INT_FGL_PMM_RPL.ORGTYPEE typ ON typ.ORG_TYPE_CODE=dtl.ORG_TYPE_CODE
            --LEFT JOIN BASATOEE ato ON mst.ORG_LVL_CHILD=ato.ORG_LVL_CHILD
            --LEFT JOIN BASAHREE ahr ON ahr.ATR_HEADER_DESC='Store Type' AND ahr.ATR_HDR_TECH_KEY=ato.ATR_HDR_TECH_KEY
            --LEFT JOIN BASACDEE acd ON acd.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY
            left join 
            (SELECT ato.ORG_LVL_CHILD, ahr.ATR_HEADER_DESC, acd.ATR_CODE, acd.ATR_CODE_DESC
                        FROM INT_FGL_PMM_RPL.BASATOEE ato
                        INNER JOIN INT_FGL_PMM_RPL.BASATYEE aty ON ato.atr_typ_tech_key = aty.atr_typ_tech_key
                        INNER JOIN INT_FGL_PMM_RPL.BASAHREE ahr ON ato.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY
                        INNER JOIN INT_FGL_PMM_RPL.BASACDEE acd ON acd.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY AND acd.ATR_COD_TECH_KEY=ato.ATR_COD_TECH_KEY
                        WHERE ahr.ATR_HEADER_DESC = 'Store Type'
                              and aty.atr_type_desc = 'Store Type'
                              and ahr.app_func = 'ORG'
                              and ahr.app_name = 'BAS')basa
            on mst.org_lvl_child=basa.org_lvl_child
            left join 
            (SELECT ato.ORG_LVL_CHILD, ahr.ATR_HEADER_DESC, acd.ATR_CODE, acd.ATR_CODE_DESC
                        FROM INT_FGL_PMM_RPL.BASATOEE ato
                        INNER JOIN INT_FGL_PMM_RPL.BASATYEE aty ON ato.atr_typ_tech_key = aty.atr_typ_tech_key
                        INNER JOIN INT_FGL_PMM_RPL.BASAHREE ahr ON ato.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY
                        INNER JOIN INT_FGL_PMM_RPL.BASACDEE acd ON acd.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY AND acd.ATR_COD_TECH_KEY=ato.ATR_COD_TECH_KEY
                        WHERE ahr.ATR_HEADER_DESC = 'Store Status'
                              and aty.atr_type_desc = 'Store Status'
                              and ahr.app_func = 'ORG'
                              and ahr.app_name = 'BAS')basa1
            on mst.org_lvl_child=basa1.org_lvl_child
            LEFT JOIN INT_FGL_PMM_RPL.BASCOOEE ctry ON dtl.CNTRY_LVL_CHILD = ctry.CNTRY_LVL_CHILD
            LEFT JOIN INT_FGL_PMM_RPL.BASADREE adr ON  dtl.BAS_ADD_KEY = adr.BAS_ADD_KEY
            LEFT JOIN INT_FGL_PMM_RPL.BASVALEE val ON val.ENTITY_NAME = 'ORGMSTEE' AND val.TECH_KEY1 = mst.ORG_LVL_CHILD AND val.FIELD_CODE = '15' -- WMSWHSE
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                       ) lon 
                          ON lon.entity_name = 'ORGMSTEE' 
                             AND lon.tech_key1 = mst.org_lvl_child 
                             AND lon.field_code = '45' -- Longitude 
                   left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                  ) lat 
                          ON lat.entity_name = 'ORGMSTEE' 
                             AND lat.tech_key1 = mst.org_lvl_child 
                             AND lat.field_code = '40' -- Latitude 
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                       ) sq_ftg 
                          ON sq_ftg .entity_name = 'ORGMSTEE' 
                             AND sq_ftg .tech_key1 = mst.org_lvl_child 
                             AND sq_ftg .field_code = '20' -- Square Footage
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                  ) tz
                          ON tz.entity_name = 'ORGMSTEE' 
                             AND tz.tech_key1 = mst.org_lvl_child 
                             AND tz.field_code = '63' -- Time Zone
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                  ) dlt
                          ON dlt.entity_name = 'ORGMSTEE' 
                             AND dlt.tech_key1 = mst.org_lvl_child 
                             AND dlt.field_code = '64' -- Day Light Saving flag
            WHERE mst.ORG_LVL_ID = 1
        """


        def stores = []
        def result = connection.eachRow(sql) {

            Store store = new Store()
            store.name = it.ORG_NAME_FULL
            store.storeNumber = it.ORG_LVL_NUMBER
            store.address = it.BAS_ADDR_1 + " " + it.BAS_ADDR_2
            store.city = it.BAS_CITY

            if (it.LONGITUDE && it.LONGITUDE) {
                store.longitude = Double.parseDouble(it.LONGITUDE)
                store.latitude = Double.parseDouble(it.LATITUDE)
            }
            else{
                store.longitude = 0
                store.latitude = 0
            }



            stores << store

        }

        return stores
    }

    @Override
    List<Store> getAllStoresForProvince(String province) {

        Sql connection = new Sql(dataSource)


        def sql = """
            SELECT    
            mst.ORG_LVL_CHILD  
            , mst.ORG_LVL_NUMBER  
            , mst.ORG_NAME_SHORT 
            , mst.ORG_NAME_FULL  
            , dtl.ORG_DATE_OPENED  
            , dtl.ORG_DATE_CLOSED 
            , dtl.ORG_LVL_ACTIVE
            , TRIM(adr.BAS_ADDR_1) AS BAS_ADDR_1 
            , TRIM(adr.BAS_ADDR_2) AS BAS_ADDR_2  
            , TRIM(adr.BAS_ADDR_3) AS BAS_ADDR_3  
            , TRIM(adr.BAS_CITY) AS BAS_CITY  
            , TRIM(adr.BAS_STATE) AS BAS_STATE  
            , TRIM(adr.BAS_ZIP) AS BAS_ZIP  
            , TRIM(ctry.CNTRY_CODE) AS CNTRY_CODE  
            , TRIM(adr.BAS_ISD_FAX) AS BAS_ISD_FAX  
            , TRIM(adr.BAS_ISD) AS BAS_ISD  
            , TRIM(adr.BAS_AREA) AS BAS_AREA 
            , TRIM(adr.BAS_AREA_FAX) AS BAS_AREA_FAX  
            , TRIM(adr.BAS_PHONE_NUMB) AS BAS_PHONE_NUMB  
            , TRIM(adr.BAS_FAX_NUMBER) AS BAS_FAX_NUMBER 
            , TRIM(adr.VPC_EMAIL) AS VPC_EMAIL  
            , dtl.ORG_MANAGER_NAME  
            , val.VALUE AS WMSWHSE
            , mst.ORG_IS_STORE  
            ,basa.ATR_CODE_DESC STORE_TYPE
            ,lon.VALUE as LONGITUDE
            ,lat.VALUE as LATITUDE
            ,sq_ftg.VALUE as SQUARE_FOOTAGE
            ,CASE to_char(tz.VALUE)
              WHEN '10' THEN 'PST'
              WHEN '15' THEN 'MST'
              WHEN '20' THEN 'CST'
              WHEN '25' THEN 'EST'
              WHEN '30' THEN 'AST'
              WHEN '35' THEN 'NST' 
              END AS TIMEZONE
            ,dlt.VALUE as DAYLIGHT_SAVING
            ,basa1.ATR_CODE STORE_STATUS_CODE
            ,basa1.ATR_CODE_DESC STORE_STATUS
            ,typ.ORG_TYPE_CODE as ORG_TYPE_CODE
            ,typ.ORG_TYPE_DESC AS ORG_TYPE_DESC
            ,mst.CURR_CODE CURR_CODE
            FROM INT_FGL_PMM_RPL.ORGMSTEE mst
            INNER JOIN INT_FGL_PMM_RPL.ORGDTLEE dtl ON mst.ORG_LVL_CHILD = dtl.ORG_LVL_CHILD
            LEFT JOIN INT_FGL_PMM_RPL.ORGTYPEE typ ON typ.ORG_TYPE_CODE=dtl.ORG_TYPE_CODE
            --LEFT JOIN BASATOEE ato ON mst.ORG_LVL_CHILD=ato.ORG_LVL_CHILD
            --LEFT JOIN BASAHREE ahr ON ahr.ATR_HEADER_DESC='Store Type' AND ahr.ATR_HDR_TECH_KEY=ato.ATR_HDR_TECH_KEY
            --LEFT JOIN BASACDEE acd ON acd.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY
            left join 
            (SELECT ato.ORG_LVL_CHILD, ahr.ATR_HEADER_DESC, acd.ATR_CODE, acd.ATR_CODE_DESC
                        FROM INT_FGL_PMM_RPL.BASATOEE ato
                        INNER JOIN INT_FGL_PMM_RPL.BASATYEE aty ON ato.atr_typ_tech_key = aty.atr_typ_tech_key
                        INNER JOIN INT_FGL_PMM_RPL.BASAHREE ahr ON ato.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY
                        INNER JOIN INT_FGL_PMM_RPL.BASACDEE acd ON acd.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY AND acd.ATR_COD_TECH_KEY=ato.ATR_COD_TECH_KEY
                        WHERE ahr.ATR_HEADER_DESC = 'Store Type'
                              and aty.atr_type_desc = 'Store Type'
                              and ahr.app_func = 'ORG'
                              and ahr.app_name = 'BAS')basa
            on mst.org_lvl_child=basa.org_lvl_child
            left join 
            (SELECT ato.ORG_LVL_CHILD, ahr.ATR_HEADER_DESC, acd.ATR_CODE, acd.ATR_CODE_DESC
                        FROM INT_FGL_PMM_RPL.BASATOEE ato
                        INNER JOIN INT_FGL_PMM_RPL.BASATYEE aty ON ato.atr_typ_tech_key = aty.atr_typ_tech_key
                        INNER JOIN INT_FGL_PMM_RPL.BASAHREE ahr ON ato.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY
                        INNER JOIN INT_FGL_PMM_RPL.BASACDEE acd ON acd.ATR_HDR_TECH_KEY=ahr.ATR_HDR_TECH_KEY AND acd.ATR_COD_TECH_KEY=ato.ATR_COD_TECH_KEY
                        WHERE ahr.ATR_HEADER_DESC = 'Store Status'
                              and aty.atr_type_desc = 'Store Status'
                              and ahr.app_func = 'ORG'
                              and ahr.app_name = 'BAS')basa1
            on mst.org_lvl_child=basa1.org_lvl_child
            LEFT JOIN INT_FGL_PMM_RPL.BASCOOEE ctry ON dtl.CNTRY_LVL_CHILD = ctry.CNTRY_LVL_CHILD
            LEFT JOIN INT_FGL_PMM_RPL.BASADREE adr ON  dtl.BAS_ADD_KEY = adr.BAS_ADD_KEY
            LEFT JOIN INT_FGL_PMM_RPL.BASVALEE val ON val.ENTITY_NAME = 'ORGMSTEE' AND val.TECH_KEY1 = mst.ORG_LVL_CHILD AND val.FIELD_CODE = '15' -- WMSWHSE
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                       ) lon 
                          ON lon.entity_name = 'ORGMSTEE' 
                             AND lon.tech_key1 = mst.org_lvl_child 
                             AND lon.field_code = '45' -- Longitude 
                   left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                  ) lat 
                          ON lat.entity_name = 'ORGMSTEE' 
                             AND lat.tech_key1 = mst.org_lvl_child 
                             AND lat.field_code = '40' -- Latitude 
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                       ) sq_ftg 
                          ON sq_ftg .entity_name = 'ORGMSTEE' 
                             AND sq_ftg .tech_key1 = mst.org_lvl_child 
                             AND sq_ftg .field_code = '20' -- Square Footage
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                  ) tz
                          ON tz.entity_name = 'ORGMSTEE' 
                             AND tz.tech_key1 = mst.org_lvl_child 
                             AND tz.field_code = '63' -- Time Zone
            left join (SELECT * 
                              FROM   INT_FGL_PMM_RPL.basvalee 
                              --WHERE  change_action_flg <> 'D'
                  ) dlt
                          ON dlt.entity_name = 'ORGMSTEE' 
                             AND dlt.tech_key1 = mst.org_lvl_child 
                             AND dlt.field_code = '64' -- Day Light Saving flag
            WHERE mst.ORG_LVL_ID = 1
            and BAS_STATE='${province}' 
        """

        println sql

        def stores = []
        def result = connection.eachRow(sql) {

            Store store = new Store()
            store.name = it.ORG_NAME_FULL
            store.storeNumber = it.ORG_LVL_NUMBER
            store.address = it.BAS_ADDR_1 + " " + it.BAS_ADDR_2
            store.city = it.BAS_CITY

            if (it.LONGITUDE && it.LONGITUDE) {
                store.longitude = Double.parseDouble(it.LONGITUDE)
                store.latitude = Double.parseDouble(it.LATITUDE)
            }
            else{
                store.longitude = 0
                store.latitude = 0
            }

            stores << store

        }

        return stores
    }



    @Override
    List<DistanceResult> getAllStoresNearby(String storeNumber) {



        def distanceResult = []

        if (!cachedStores){
            cachedStores = getAllStores()


        }

        Store store = cachedStores.find{it.storeNumber==storeNumber}

        // get it from the list

        if (!spatialSearch){
            String indexPath = "~/fgl_stores_geo_spatial_index";
            spatialSearch = new SpatialSearch(indexPath);
            spatialSearch.setUpIndex(indexPath,cachedStores)
        }

        def searchResult = spatialSearch.search(store.longitude,store.latitude,defaultDistance)



        searchResult.keySet().each{storeNumberKey->

            def tempStore = cachedStores.find{it.storeNumber==storeNumberKey}
            def tempDistance = searchResult[storeNumberKey]

            distanceResult << new DistanceResult(store:tempStore,distance:tempDistance,)


        }


        distanceResult





    }

}
