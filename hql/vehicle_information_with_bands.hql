use psa_shark;
drop table if exists vehicle_dimension_bands;
create table vehicle_dimension_bands as 
select 
     vehicleid,
     vehicleageindays,
     mileage,
     stockage,     
     TotalDamagesNetPrice,
     CASE WHEN vehicleageindays/7 >= 0 AND vehicleageindays/7 < 3 THEN 'under 3'
             WHEN vehicleageindays/7 >= 3 AND vehicleageindays/7 < 8 THEN '3 - 8'
             WHEN vehicleageindays/7 >= 8 AND vehicleageindays/7 < 14 THEN '9 - 14'
             WHEN vehicleageindays/7 >= 14 AND vehicleageindays/7 < 20 THEN '15 - 20'
             WHEN vehicleageindays/7 >= 20 AND vehicleageindays/7 < 26 THEN '21 - 26'
             WHEN vehicleageindays/7 >= 26 AND vehicleageindays/7 < 32 THEN '27 - 32'
             WHEN vehicleageindays/7 >= 32 AND vehicleageindays/7 < 38 THEN '33 - 38'
             WHEN vehicleageindays/7 >= 38 AND vehicleageindays/7 < 44 THEN '39 - 44'
             WHEN vehicleageindays/7 >= 44 AND vehicleageindays/7 < 50 THEN '45 - 50'
             WHEN vehicleageindays/7 >= 50 AND vehicleageindays/7 < 56 THEN '51 - 56'
             WHEN vehicleageindays/7 >= 56 AND vehicleageindays/7 < 62 THEN '57 - 62'
             WHEN vehicleageindays/7 >= 62 AND vehicleageindays/7 < 68 THEN '63 - 68'
             WHEN vehicleageindays/7 >= 68 AND vehicleageindays/7 < 74 THEN '69 - 74'
             WHEN vehicleageindays/7 >= 74 AND vehicleageindays/7 < 80 THEN '75 - 80'
             WHEN vehicleageindays/7 >= 80 AND vehicleageindays/7 < 86 THEN '81 - 86'
             WHEN vehicleageindays/7 >= 86 AND vehicleageindays/7 < 92 THEN '87 - 92'
             ELSE 'over 92'
     END as ageInWeeksBandName,
     CASE WHEN vehicleageindays/30 >= 0 AND vehicleageindays/30 < 3 THEN 0
             WHEN vehicleageindays/7 >= 3 AND vehicleageindays/7 < 8 THEN 1
             WHEN vehicleageindays/7 >= 8 AND vehicleageindays/7 < 14 THEN 2
             WHEN vehicleageindays/7 >= 14 AND vehicleageindays/7 < 20 THEN 3
             WHEN vehicleageindays/7 >= 20 AND vehicleageindays/7 < 26 THEN 4
             WHEN vehicleageindays/7 >= 26 AND vehicleageindays/7 < 32 THEN 5
             WHEN vehicleageindays/7 >= 32 AND vehicleageindays/7 < 38 THEN 6
             WHEN vehicleageindays/7 >= 38 AND vehicleageindays/7 < 44 THEN 7
             WHEN vehicleageindays/7 >= 44 AND vehicleageindays/7 < 50 THEN 8
             WHEN vehicleageindays/7 >= 50 AND vehicleageindays/7 < 56 THEN 9
             WHEN vehicleageindays/7 >= 56 AND vehicleageindays/7 < 62 THEN 10
             WHEN vehicleageindays/7 >= 62 AND vehicleageindays/7 < 68 THEN 11
             WHEN vehicleageindays/7 >= 68 AND vehicleageindays/7 < 74 THEN 12
             WHEN vehicleageindays/7 >= 74 AND vehicleageindays/7 < 80 THEN 13
             WHEN vehicleageindays/7 >= 80 AND vehicleageindays/7 < 86 THEN 14
             WHEN vehicleageindays/7 >= 86 AND vehicleageindays/7 < 92 THEN 15
             ELSE 16
     END as ageInWeeksBandId,
     CASE WHEN  VI.stockage/7 <8 THEN 'under 1 '
            WHEN VI.stockage/7 >=8 AND VI.stockage/7 <15 THEN '1 - 2'
            WHEN VI.stockage/7 >=15 AND VI.stockage/7 <22 THEN '2 - 3'
            WHEN VI.stockage/7 >=22 AND VI.stockage/7 <29 THEN '3 - 4'
            WHEN VI.stockage/7 >=29 AND VI.stockage/7 <36 THEN '4 - 5'
            WHEN VI.stockage/7 >=36 AND VI.stockage/7 <43 THEN '5 - 6'
            WHEN VI.stockage/7 >=43 AND VI.stockage/7 <50 THEN '6 - 7'
            WHEN VI.stockage/7 >=50 AND VI.stockage/7 <57 THEN '7 - 8'
            WHEN VI.stockage/7 >=57 AND VI.stockage/7 <64 THEN '8 - 9'
            WHEN VI.stockage/7 >=64 AND VI.stockage/7 <71 THEN '9 - 10'
            WHEN VI.stockage/7 >=71 AND VI.stockage/7 <78 THEN '10 - 11'
            WHEN VI.stockage/7 >=78 AND VI.stockage/7 <85 THEN '11 - 12'
            WHEN VI.stockage/7 >=85 AND VI.stockage/7 <92 THEN '12 - 13'
            WHEN VI.stockage/7 >=92 AND VI.stockage/7 <99 THEN '13 - 14'
            WHEN VI.stockage/7 >=99 AND VI.stockage/7 <106 THEN '14 - 15'
            WHEN VI.stockage/7 >=106 AND VI.stockage/7 <113 THEN '15 - 16'
            WHEN VI.stockage/7 >=113 AND VI.stockage/7 <100000 THEN 'over 16'
      end as stockageWeeksBandName,
      CASE  WHEN VI.stockage/7 <8 THEN 0
            WHEN VI.stockage/7 >=8 AND VI.stockage/7 <15 THEN 1
            WHEN VI.stockage/7 >=15 AND VI.stockage/7 <22 THEN 2
            WHEN VI.stockage/7 >=22 AND VI.stockage/7 <29 THEN 3
            WHEN VI.stockage/7 >=29 AND VI.stockage/7 <36 THEN 4
            WHEN VI.stockage/7 >=36 AND VI.stockage/7 <43 THEN 5
            WHEN VI.stockage/7 >=43 AND VI.stockage/7 <50 THEN 6
            WHEN VI.stockage/7 >=50 AND VI.stockage/7 <57 THEN 7
            WHEN VI.stockage/7 >=57 AND VI.stockage/7 <64 THEN 8
            WHEN VI.stockage/7 >=64 AND VI.stockage/7 <71 THEN 9
            WHEN VI.stockage/7 >=71 AND VI.stockage/7 <78 THEN 10
            WHEN VI.stockage/7 >=78 AND VI.stockage/7 <85 THEN 11
            WHEN VI.stockage/7 >=85 AND VI.stockage/7 <92 THEN 12
            WHEN VI.stockage/7 >=92 AND VI.stockage/7 <99 THEN 13
            WHEN VI.stockage/7 >=99 AND VI.stockage/7 <106 THEN 14
            WHEN VI.stockage/7 >=106 AND VI.stockage/7 <113 THEN 15
            WHEN VI.stockage/7 >=113 AND VI.stockage/7 <100000 THEN 16
      end as stockageWeeksBandId,
      CASE WHEN  VI.totaldamagesnetprice <99 THEN 'under 100'
           WHEN VI.totaldamagesnetprice >=100 AND VI.totaldamagesnetprice <500 THEN '100 - 500'
           WHEN VI.totaldamagesnetprice >=500 AND VI.totaldamagesnetprice <750 THEN '501 - 750'
           WHEN VI.totaldamagesnetprice >=750 AND VI.totaldamagesnetprice <100000 THEN 'over 750'
      end as damagesBandName,
      case WHEN  VI.totaldamagesnetprice <100 THEN 0
           WHEN VI.totaldamagesnetprice >=100 AND VI.totaldamagesnetprice <500 THEN 1
           WHEN VI.totaldamagesnetprice >=500 AND VI.totaldamagesnetprice <750 THEN 2
           WHEN VI.totaldamagesnetprice >=750 AND VI.totaldamagesnetprice <100000 THEN 3
      end as damagesBandId,
     case WHEN  VI.mileage <10000 THEN '0-10 000'
          WHEN VI.mileage >=10000 AND VI.mileage <20000 THEN '10,001 - 20,000'
          WHEN VI.mileage >=20000 AND VI.mileage <30000 THEN '20,001 - 30,000'
          WHEN VI.mileage >=30000 AND VI.mileage <40000 THEN '30,001 - 40,000'
          WHEN VI.mileage >=40000 AND VI.mileage <50000 THEN '40,001 - 50,000'
          WHEN VI.mileage >=50000 AND VI.mileage <75000 THEN '50,001 - 75,000'
          WHEN VI.mileage >=75000 AND VI.mileage <100000 THEN '75,001 - 100,000'
          WHEN VI.mileage >=100000 AND VI.mileage <150000 THEN '100,001 - 150,000'
          WHEN VI.mileage >=150000 AND VI.mileage <999999 THEN 'over 150,000'
      end mileageBandName,
    case
            WHEN  VI.mileage <10000 THEN 0
            WHEN VI.mileage >=10000 AND VI.mileage <20000 THEN 1
            WHEN VI.mileage >=20000 AND VI.mileage <30000 THEN 2
            WHEN VI.mileage >=30000 AND VI.mileage <40000 THEN 3
            WHEN VI.mileage >=40000 AND VI.mileage <50000 THEN 4
            WHEN VI.mileage >=50000 AND VI.mileage <75000 THEN 5
            WHEN VI.mileage >=75000 AND VI.mileage <100000 THEN 6
            WHEN VI.mileage >=100000 AND VI.mileage <150000 THEN 7
            WHEN VI.mileage >=150000 AND VI.mileage <999999 THEN 8
end mileageBandId
     from 
	(select VI.vehicleId, 
                CASE WHEN COALESCE(VI.IsUpstream,0) = 1 THEN CASE WHEN DATEDIFF(VAA.StockDate, from_unixtime(unix_timestamp())) > 0 THEN DATEDIFF( VAA.StockDate, from_unixtime(unix_timestamp())) ELSE 0 END
		ELSE DATEDIFF(VAA.StockDate, from_unixtime(unix_timestamp())) END AS StockAge,
                VAA.TotalDamagesNetPrice,
                VH.Mileage 
	from 
		psa.VehicleInstance_stg 
                LEFT JOIN psa.VehicleHeader_stg VH.ID = VI.VehicleID   
                LEFT JOIN psa.VehicleAdditionalAttributes_stg VAA ON VAA.VehicleINstanceID = VI.ID 

 ) t;
