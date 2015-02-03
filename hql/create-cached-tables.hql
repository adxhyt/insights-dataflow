use psa_shark;
drop table if exists psa_shark.Buyervehiclepurchase_stg_cached;
CREATE TABLE IF NOT EXISTS Buyervehiclepurchase_stg_cached
  AS SELECT 
  buyervehiclepurchaseid ,
  deferpaymentdays ,
  sorreturncutoffdays ,
  paymenttypeid ,
  buyerid ,
  vehicleid ,
  bulkpackid ,
  campaignid ,
  vehiclepurchasedt ,
  deliverytypeid ,
  deliverychargeamt ,
  buyerdeliverylocationid ,
  paymentreceivedflg ,
  purchaseschemeid ,
  netpriceamt ,
  vatamt ,
  campaigndiscount ,
  vehicledeliverystatusid ,
  vehiclesparekeystatusid ,
  createduserid ,
  createddt ,
  modifieduserid ,
  modifieddt ,
  isactive ,
  timestamp ,
  buyercallid ,
  saletypeid ,
  referenceinvoice ,
  vehicleallocationid ,
  salessessionstepid ,
  vehicleactualprice ,
  salechanneltypeid ,
  salechannelid ,
  directsalesuserid ,
  buyervendorid ,
  vehiclevendorid ,
  isinternationalsale ,
  directsaleid ,
  vendorcurrencyid ,
  paymenttypecode ,
  isagentpurchase ,
  transactionstatusid ,
  completiondate ,
  deliverylocationcode ,
  purchasereasonid 
  FROM psa.Buyervehiclepurchase_stg;

drop table if exists Buyer_stg_cached;
CREATE TABLE IF NOT EXISTS Buyer_stg_cached
  AS SELECT 
  id ,
  name ,
  buyercode ,
  buyerstatusid ,
  parentbuyerid ,
  buyerregionid ,
  buyertypeid ,
  buyersizeid ,
  tradingstatusflg ,
  defaultpaymenttypeid ,
  deactivationreason ,
  logonstatusflg ,
  buyernotes ,
  dateinactive ,
  defaultlanguageid ,
  createduserid ,
  createdate ,
  modifieduserid ,
  modifieddt ,
  isactive ,
  companyno ,
  vatregno ,
  tradingname ,
  timestamp ,
  salesrepresentativeid ,
  notes ,
  purchasegroupid ,
  categoryid ,
  masteraccountcode ,
  laisoncode ,
  buyerauthorisationstatusid ,
  maximumnumberofusers ,
  issalestermsaccepted ,
  buyerexportinformationid ,
  currencyid ,
  organisationcode ,
  vat_global ,
  vat_local ,
  isbasketprotectionenabled ,
  maxbasketprotectionvehicles ,
  basketprotectionvehiclesduration ,
  maxbasketprotectionpacks ,
  basketprotectionpacksduration ,
  issalestermsuploaded ,
  previouparentbuyerid ,
  allowonlydefaultpaymentmethod ,
  mafs 
  FROM psa.Buyer_stg;


drop table if exists BuyerRegion_stg_cached;
CREATE TABLE IF NOT EXISTS BuyerRegion_stg_cached
  AS SELECT 
  buyerregionid ,
  buyerregionname ,
  vendorid ,
  isactive 
  FROM psa.BuyerRegion_stg;


drop table if exists Buyerusers_stg_cached;
CREATE TABLE IF NOT EXISTS Buyerusers_stg_cached
  AS SELECT 
  id ,
  buyerid ,
  userid 
  FROM psa.Buyerusers_stg;


drop table if exists VendorBuyers_stg_cached;
CREATE  TABLE IF NOT EXISTS VendorBuyers_stg_cached
  AS SELECT 
  vendorbuyersid ,
  buyerid ,
  vendorid ,
  isactive 
  FROM psa.VendorBuyers_stg;


drop table if exists Vendor_stg_cached;
CREATE TABLE IF NOT EXISTS Vendor_stg_cached
  AS SELECT 
  id ,
  name ,
  tradingname ,
  companyno ,
  vatregno ,
  vendorurl ,
  datasourceid ,
  effectivefromdt ,
  effectivetodt ,
  livestatusflg ,
  tradingstatusflg ,
  defaultcurrencyid ,
  createduserid ,
  createdate ,
  modifieduserid ,
  modifieddt ,
  timestamp ,
  isactive ,
  code ,
  qacode ,
  mainlogo ,
  sublogo ,
  tinylogo ,
  countrylogo ,
  supportsmultichannelallocation ,
  timezonedetailid ,
  parentvendorid ,
  vendortypeid 
  FROM psa.Vendor_stg;

drop table if exists psa_shark.VehicleInstance_stg_cached;
CREATE TABLE IF NOT EXISTS VehicleInstance_stg_cached
  AS SELECT 
  id ,
  title ,
  vehicleid ,
  derivativeid ,
  vendorid ,
  buyerid ,
  exteriorcolourid ,
  interiortrimid ,
  fueltypeid ,
  transmissionid ,
  statusid ,
  vendorstatusid ,
  allocationstatusid ,
  datasourceid ,
  qacomplete ,
  comments ,
  conditionid ,
  locationid ,
  sourceid ,
  defleetdt ,
  manualpriceflg ,
  manualpricedt ,
  vatstatus ,
  isavailableforsale ,
  availableforsaledt ,
  arrivaldate ,
  inspectiondate ,
  inspector ,
  isinspectionpassed ,
  isv5available ,
  awaitingparts ,
  majordamage ,
  reserveprice ,
  estimateprice ,
  unittype ,
  siv ,
  bulkpackreservedflg ,
  reserveddate ,
  vatqualified ,
  imaged ,
  sold ,
  createduserid ,
  createddt ,
  modifieduserid ,
  modfieddt ,
  timestamp ,
  isactive ,
  dispatchdate ,
  isreserved ,
  isinbasket ,
  dateaddedtobasket ,
  useraddedtobasket ,
  companyid ,
  uniquecode ,
  countryid ,
  rflexpirydate ,
  estimatedreturnmileage ,
  autofuturacode ,
  isusedvehicle ,
  ismaintenanceavailable ,
  supplierid ,
  alternativetitle ,
  onhold ,
  onholddate ,
  approvedtypeid ,
  lotnumber ,
  laneno ,
  branchid ,
  modelyear ,
  vatqualifyingtype ,
  issimulcast ,
  interiorcolourid ,
  hasinbasketpreviously ,
  buildyear ,
  iswarrantedmileage ,
  reconditioned ,
  isupstream 
  FROM psa.VehicleInstance_stg;
quit;
