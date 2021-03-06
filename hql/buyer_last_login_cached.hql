use psa_shark;
DROP TABLE IF EXISTS buyer_last_login_cached;
CREATE TABLE buyer_last_login_cached AS
SELECT U.id,
        U.username,
        U.createddate,
        U.loggedints,
        Buyer.id as buyerid,
        Buyer.name as buyername,
        BuyerType.BuyerTypeId,
        BuyerType.BuyerTypeName
from (select id, username, createddate, MAX(unix_timestamp(loggedindate)) as loggedints from  psa.useraudit_stg group by id, username, createddate) U
join psa.BuyerUsers_stg BU ON BU.userid=U.id
join psa.Buyer_stg Buyer ON Buyer.Id = BU.buyerid
LEFT OUTER  JOIN psa.BuyerType_stg BuyerType ON Buyer.BuyerTypeId = BuyerType.BuyerTypeId;
CACHE TABLE buyer_last_login_cached;
