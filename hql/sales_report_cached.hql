!connect ${connectString}/psa_shark dummy dummy org.apache.hive.jdbc.HiveDriver
use psa_shark;
DROP TABLE IF EXISTS sales_report_cached;
CREATE TABLE sales_report_cached
 AS SELECT VI.Make,
                           VI.Makeref, 
                           VI.Registration,
                           VI.Chassis,
                           VI.Derivative,
                           VI.derivativeid,
                           VI.RegistrationDate,
                           VI.exteriorcolour,
                           VI.createddt as CreationDate,
                           BVP.SaleChannelId,
                           VI.SaleChannel,
                           V.name as VendorTradingName,
                           VI.VendorID, 
                           VI.VehicleAgeInDays,
                           BVP.VehiclePurchaseDt AS SoldDate,
                           unix_timestamp( BVP.VehiclePurchaseDt) AS SoldDateTs, 
                           CASE VI.VatQualified
                            WHEN true THEN 'Marginal'
                                WHEN false THEN 'VAT Qualifying'
                                WHEN NULL THEN ''
                           END AS VatQualified,
                           (BVP.NetPriceAmt + BVP.VATAmt) AS SoldPrice,
                            BPC.BuyerPremium AS BuyerPremium,
                            GDC.Delivery AS Delivery,
                            B.Name AS Buyer,
                            B.ID as BuyerID,
                                B.BuyerCode AS BuyerCode,
                                regexp_replace(COALESCE(AD.NameNo, ''), ',', ' ') + ' ' + regexp_replace(COALESCE(AD.Addressline1, ''), ',', ' ') + ' '
                                + regexp_replace(COALESCE(AD.Addressline2, ''), ',', ' ') + ' ' + regexp_replace(COALESCE(AD.Town, ''), ',', ' ') + ' '
                                + regexp_replace(COALESCE(AD.PostCode, ''), ',', ' ') + ' ' + regexp_replace(COALESCE(AD.County, ''), ',', ' ') + ' '
                                + regexp_replace(COALESCE(CU.Name, ''), ',', ' ') + ' ' AS DeliveryLocation,
                           CASE BVP.IsActive
                                WHEN true THEN 'N'
                                WHEN false THEN 'Y'
                                WHEN NULL THEN ''
                           END AS ActiveSale ,
                           VI.Model,
                           VI.Code,
                           VI.ModelYear,
                           VI.ModelRef AS Model_Code,
                           VI.Mileage,
                           VI.DaysOnSale,
                           VI.AuctionPrice,
                           VI.Transmission, 
                           VI.Transmissionid, 
                           VI.Stockage, 
                           U.Description AS Vehicle_Type,
                           SalesTacticSession.sessionname AS SalesSession,
                           SalesTacticSession.sessionid AS SalesSessionID,
                           SalesTacticSession.tacticname AS TacticName,
                           SalesTacticSession.tacticid AS TacticId,
                           case when BVP.directsaleid > 0 then 'Direct Sale' else 
                           CommercialConcept.salechanneltypename end as CommercialConceptName,
                           CU.ID as CountryId,
                           CU.Name as CountryName,
                           VI.totaldamagesnetprice AS totaldamagesnetprice,
                           VI.fueltype AS fueltype,
                           VI.vehicleid,
                           VI.vin,
                           VI.sourceid,
                           Source.sourcename,
                           BuyerType.BuyerTypeId,
                           BuyerType.BuyerTypeName,
                           BVP.NetPriceAmt AS PriceExcludingVat,
                           BVP.directsaleid as directsaleid, 
                           Company.name as SellerName,
                           Company.id as SellerId,
                           year(BVP.VehiclePurchaseDt) as SoldYear,
                           month(BVP.VehiclePurchaseDt) as SoldMonth,
                           VDB.ageinweeksbandname,
                           VDB.ageinweeksbandid,
                           VDB.stockageweeksbandname,
                           VDB.stockageweeksbandid,
                           VDB.damagesbandname,
                           VDB.damagesbandid, 
                           VDB.mileagebandname,
                           VDB.mileagebandid,
                           BC.name as buyercountry,
                           BC.id as buyercountryid,
                           L.id as locationid,
                           L.locationname as locationname
                           CommercialConcept.SaleChannelTypeID as CommercialConceptTypeId 
from  
   psa.VehicleInformation_stg VI
   INNER JOIN psa.BuyerVehiclePurchase_stg BVP ON VI.VehicleInstanceID = BVP.VehicleID and year(BVP.VehiclePurchaseDt) not in(1900)
   INNER JOIN psa_shark.vehicle_dimension_bands VDB  ON VI.VehicleInstanceID = VDB.VehicleInstanceId
   INNER JOIN psa.Vendor_stg V ON V.ID = VI.vendorid
   LEFT OUTER JOIN psa.location_stg L on L.vendorid=V.id
   LEFT OUTER JOIN (select t.vehicleinstanceid as VehicleInstanceID, buyerpremiumcharge as BuyerPremium  from psa.buyerpremiumcharge_stg t limit 1) BPC ON BPC.VehicleInstanceID = BVP.VehicleID
   LEFT OUTER JOIN (select t.vehicleinstanceid as VehicleInstanceID, deliverycharges as Delivery  from psa.getdeliverycharges t limit 1) GDC ON GDC.VehicleInstanceID = BVP.VehicleID
   LEFT OUTER JOIN psa.vendorusers_stg VU on BVP.createduserid=VU.userid and VI.vendorid=VU.vendorid
   LEFT OUTER JOIN psa.user_stg UU ON VU.userid=UU.id
   LEFT OUTER  JOIN psa.Buyer_stg B ON B.ID = BVP.buyerid
   LEFT OUTER  JOIN psa.BuyerAddress_stg BAD ON BAD.buyerid = BVP.buyerid
   LEFT OUTER  JOIN psa.Address_stg BD ON BD.ID = BAD.addressid
   LEFT OUTER  JOIN psa.Country_stg BC ON BC.id = BD.countryid
   LEFT OUTER  JOIN psa.BuyerType_stg BuyerType ON B.BuyerTypeId = BuyerType.BuyerTypeId
   LEFT OUTER  JOIN psa.Address_stg AD ON AD.ID = BVP.BuyerDeliveryLocationID
   LEFT OUTER  JOIN psa.Country_stg CU ON  AD.CountryID = CU.ID
   LEFT OUTER  JOIN psa.UnitType_stg U ON U.ID = VI.UnitType
   LEFT OUTER  JOIN psa.SaleChannelTypeMaster_stg  CommercialConcept ON CommercialConcept.SaleChannelTypeID = BVP.SaleChannelTypeId 
   LEFT OUTER JOIN psa.source_stg Source on Source.sourceid = VI.sourceid
   LEFT OUTER  JOIN psa.company_stg Company on VI.vendorid = Company.VendorId
   LEFT OUTER  JOIN psa_shark.sales_sessions_tactic_cached SalesTacticSession ON BVP.salessessionstepid = SalesTacticSession.stepid;
cache table sales_report_cached;
