select * from customer;
select * from distance;
select * from ofd_temp;
select * from fleet;
select * from ofd_final;
select * from ofd_final_new;
select * from orders;
select * from products;
select * from vendor_rate_card;
select * from vendor_card_final;

-- Assuming your table name is 'ofd_final_new'

DROP TABLE IF EXISTS ofd_temp;
CREATE TABLE ofd_temp AS
SELECT * FROM ofd_final_new
ORDER BY RAND(42) -- Use any integer as the seed value
LIMIT 120000;

/* RFM Analysis starts */

-- Create a new customer_rfm table
DROP TABLE IF EXISTS customer_rfm;
Create table customer_rfm
as (select * from customer);

ALTER TABLE customer_rfm
ADD COLUMN Recency_Rank INT,
ADD COLUMN Frequency_Rank INT,
ADD COLUMN Monetary_Rank INT;

-- Recency
WITH RecencyCTE AS (
    SELECT 
      customer_id,
      MAX(order_placed_date) AS last_order_date,
      RANK() OVER (ORDER BY MAX(order_placed_date) DESC) AS recency_rank
    FROM orders
    GROUP BY customer_id
)

-- Frequency
, FrequencyCTE AS (
    SELECT 
      customer_id,
      COUNT(DISTINCT order_number) AS order_count,
      RANK() OVER (ORDER BY COUNT(DISTINCT order_number) DESC) AS frequency_rank
    FROM orders
    GROUP BY customer_id
)

-- Monetary Value
, MonetaryCTE AS (
    SELECT 
      subquery.customer_id,
      SUM(subquery.price * subquery.order_count) AS total_monetary_value,
      RANK() OVER (ORDER BY SUM(subquery.price * subquery.order_count) DESC) AS monetary_rank
    FROM (
      SELECT 
        ofd.customer_id,
        p.price,
        COUNT(DISTINCT ofd.order_number) AS order_count
      FROM ofd_temp AS ofd
      JOIN products p ON ofd.sku_code = p.sku_code
      GROUP BY ofd.customer_id, p.price
    ) AS subquery
    GROUP BY subquery.customer_id
)

-- Update ranks in customer table
UPDATE customer_rfm c
JOIN RecencyCTE r ON c.Customer_ID = r.customer_id
JOIN FrequencyCTE f ON c.Customer_ID = f.customer_id
JOIN MonetaryCTE m ON c.Customer_ID = m.customer_id
SET
  c.Recency_Rank = r.recency_rank,
  c.Frequency_Rank = f.frequency_rank,
  c.Monetary_Rank = m.monetary_rank;
 
-- Add RFM_Metric column to customer table
ALTER TABLE customer_rfm
ADD COLUMN RFM_Metric DOUBLE;

UPDATE customer_rfm c
SET RFM_Metric = (
    SELECT COALESCE(Recency_Rank, 0) * COALESCE(Frequency_Rank, 0) * COALESCE(Monetary_Rank, 0)
);

-- Normalize RFM_Metric between 0 and 1
UPDATE customer_rfm
SET RFM_Metric = RFM_Metric / (
    SELECT MAX(RFM_Metric) FROM (
        SELECT COALESCE(Recency_Rank, 0) * COALESCE(Frequency_Rank, 0) * COALESCE(Monetary_Rank, 0) AS RFM_Metric
        FROM customer_rfm
    ) AS subquery
);

DROP TABLE IF EXISTS ZipCodesbyRFM;
create temporary table ZipCodesbyRFM as (
SELECT `Locality Zip_Code`, AVG(`RFM_Metric`) AS AvgMetric
FROM customer_rfm
GROUP BY `Locality Zip_Code`
ORDER BY AvgMetric DESC);

/* Code for top RFM Zipcodes
select * from zipcodesbyrfm
order by avgmetric desc
limit 100;


/* RFM Analysis ends */ 

/* Vendor Allocation starts */

ALTER TABLE ofd_temp CHANGE COLUMN Driver_ID orig_driver_id VARCHAR(100);

DROP TABLE IF EXISTS fleet;
CREATE  TABLE fleet AS
SELECT Driver_ID, vehicle_type, vehicle_number, fuel_chip_code, 
Vehicle_capacity_vm_weight, vehicle_model, Vendor_Id 
FROM fleet_table;

-- Assign ratings to drivers

-- Add the driver_rating column
ALTER TABLE fleet
ADD COLUMN driver_rating INT;

-- Assign random ratings to each driver
UPDATE fleet
SET driver_rating = FLOOR(4 + (RAND(40) * 7)),
 Vehicle_capacity_vm_weight=Vehicle_capacity_vm_weight/3;
 


DROP TABLE IF EXISTS vendor_card_final;
CREATE TABLE vendor_card_final AS
SELECT *,
case 
when weight_buckets='A' then '0-15'  
when weight_buckets='B' then '15-30'  
when weight_buckets='C' then '30-45'  
when weight_buckets='D' then '45+'
END AS weight_ranges,
case 
when Vendor_id='VEN-1000' then '7.8'  
when Vendor_id='VEN-2000' then '8.1'  
when Vendor_id='VEN-3000' then '8.3'  
when Vendor_id='VEN-4000' then '7.2'
when Vendor_id='VEN-5000' then '7.6'
END AS Vendor_ratings 
FROM vendor_rate_card;

-- Sanity check for checking delays per order

DROP TABLE IF EXISTS ofd_temp_1;
create temporary table ofd_temp_1 as
(
with n as (
select x.*, round(sum(volume) over (partition by order_number),2) Net_volume
from
(
select o.*,p.Volume,date_add(str_to_date(out_for_delivery_date,'%d/%m/%Y'), interval delay day) revised_delivery_date 
from ofd_temp o
left join products p
on o.SKU_CODE=p.SKU_CODE
)x
)
select *,datediff(
str_to_date(promised_delivery_date,'%d/%m/%Y'),str_to_date(order_placed_date,'%d/%m/%Y')) promised_lead_time,
datediff(
revised_delivery_date ,str_to_date(order_placed_date,'%d/%m/%Y')) actual_lead_time,
case when net_volume between 0 and 15 then 'A'
     when net_volume between 15.001 and 30 then 'B'
     when net_volume between 30.001 and 45 then 'C'
     when net_volume between 45.001 and 100 then 'D'
end as weight_buckets
FROM n
);

-- sanity check for last mile hub and locality

DROP TABLE IF EXISTS ofd_temp_2;
create temporary table ofd_temp_2 as
(
select o.*,`Distance (km)`,
case when `Distance (km)` between 0 and 5  then '0-5'
     when `Distance (km)` between 5.001 and 10  then '5-10'
     when `Distance (km)` between 10.001 and 25  then '10+'
     end as distance_category
     ,f.vendor_id as orig_vendor_id
from ofd_temp_1 o
left join customer c
on o.customer_id=c.customer_id
left join distance d
on d.`last mile hub code`= c.`last mile hub code`
and d.`Locality Zip_Code`=c.`Locality Zip_Code`
left join fleet f
on o.orig_driver_id=f.driver_id
);


-- Assume that the original allocation is based on cost

DROP TABLE IF EXISTS ofd_temp_3;
create temporary table ofd_temp_3 as
(
select * from
(
select o.*,v.lead_time as orig_vendor_lead_time,
v.Price_Per_Delivery as orig_order_delivery_price,
row_number() over (partition by awb_number order by price_per_delivery asc) rno
from ofd_temp_2 o
left join vendor_card_final v
on o.orig_vendor_id=v.vendor_id
and o.weight_buckets = v.weight_buckets
and o.distance_category=v.distance_category
)c
where rno=1
);

ALTER TABLE  ofd_temp_3
    DROP COLUMN `rno`;
    
DROP TABLE IF EXISTS ofd_non_delays;
create temporary table ofd_non_delays as
(select * from ofd_temp_3
where delay<=0);

DROP TABLE IF EXISTS ofd_delays;
create temporary table ofd_delays as
(select * from ofd_temp_3
where delay>0);

DROP TABLE IF EXISTS order_delays_company;
create temporary table order_delays_company as
(select distinct order_number 
from ofd_delays e where promised_lead_time<orig_vendor_lead_time);

DROP TABLE IF EXISTS ofd_delays_company;
create temporary table ofd_delays_company as
(select * from ofd_delays d
where order_number in (select * from order_delays_company)
);

DROP TABLE IF EXISTS ofd_delays_vendor;
create temporary table ofd_delays_vendor as
(select * from ofd_delays d
where not exists (select 1 from ofd_delays_company c where c.order_number=d.order_number)
);

DROP TABLE IF EXISTS ofd_delays_company_alloc;
Create temporary table ofd_delays_company_alloc as
(select * from
(select d.*,v.vendor_id as new_vendor_id, v.price_per_delivery new_order_delivery_price, v.lead_time new_vendor_lead_time, 
dense_rank() over (partition by awb_number order by v.price_per_delivery asc,
v.lead_time asc,v.vendor_ratings desc) rnk 
from ofd_delays_company d
left join vendor_card_final v
on d.weight_buckets = v.weight_buckets
and d.distance_category=v.distance_category 
and d.promised_lead_time>=v.lead_time
)x
where rnk=1
);

Drop table if exists ofd_delays_vendor1;
CREATE temporary TABLE ofd_delays_vendor1 LIKE ofd_delays_vendor;
INSERT INTO ofd_delays_vendor1 SELECT * FROM ofd_delays_vendor;


ALTER TABLE  ofd_delays_company_alloc
    DROP COLUMN `rnk`;

DROP TABLE IF EXISTS ofd_delays_vendor_alloc;
Create temporary table ofd_delays_vendor_alloc as
(
with group1 as 
(select count(distinct new_vendor_id) n_count, count(distinct orig_vendor_id) o_count,order_number
from
(select d.*,v.vendor_id as new_vendor_id, v.price_per_delivery new_order_delivery_price, v.lead_time new_vendor_lead_time, 
dense_rank() over (partition by awb_number order by v.price_per_delivery asc,
v.vendor_ratings desc, v.lead_time asc) rnk 
from ofd_delays_vendor1 d
left join vendor_card_final v
on d.weight_buckets = v.weight_buckets
and d.distance_category=v.distance_category 
and d.promised_lead_time>=v.lead_time
)y
group by order_number
)
select * from 
(select *,dense_rank() over(partition by awb_number order by rnk asc) new_rnk
from
(select x.*,case when g.o_count< g.n_count
then case when min(filter) over (partition by order_number,new_vendor_id)=0 
     then 0 else 1 end
else 2
end filter1
from
(select d.*,
case when orig_vendor_id=v.vendor_id then 0
     else 1 end as filter,
v.vendor_id as new_vendor_id, v.price_per_delivery new_order_delivery_price, v.lead_time new_vendor_lead_time, 
dense_rank() over (partition by awb_number order by v.price_per_delivery asc,
v.vendor_ratings desc, v.lead_time asc) rnk 
from ofd_delays_vendor d
left join vendor_card_final v
on d.weight_buckets = v.weight_buckets
and d.distance_category=v.distance_category 
and d.promised_lead_time>=v.lead_time
)x
left join group1 g
on x.order_number=g.order_number
)y
where filter1=1 or filter1=2
)z
where new_rnk=1
);

ALTER TABLE  ofd_delays_vendor_alloc
    DROP COLUMN `filter`,
    DROP COLUMN `rnk`,
    DROP COLUMN `new_rnk`,
    DROP COLUMN filter1;
    
DROP TABLE IF EXISTS ofd_vendor_alloc;
Create table ofd_vendor_alloc as
(
(select *,orig_vendor_id new_vendor_id,
orig_order_delivery_price new_order_delivery_price,
orig_vendor_lead_time new_vendor_time,
0 delay_status
from ofd_non_delays)
union all
(select *,
1 delay_status
from ofd_delays_company_alloc)
union all
(select *,
2 delay_status
from ofd_delays_vendor_alloc)
);

/* Vendor Allocation ends */

/* Driver Allocation starts */


DROP TABLE IF EXISTS alloc_order_id;
Create  table alloc_order_id as
select a.*, round(sum(net_volume) over(partition by new_vendor_id,`Last Mile Hub`,
order_placed_date order by net_volume,order_number, customer_id asc),2) Cumulative_volume
,round(sum(net_volume) over(partition by order_placed_date, new_vendor_id order by `Last Mile Hub`, net_volume, order_number, customer_id asc),2) Cumulative_volume_1
,lag(`last mile hub`,1,`last mile hub`) over (partition by order_placed_date, new_vendor_id order by `Last Mile Hub`, net_volume, order_number asc) lag_hub
,DENSE_RANK() OVER (partition by order_placed_date, new_vendor_id order by `Last Mile Hub`) AS `index`
from 
(
select order_number, max(net_volume) Net_Volume, 
max(`last mile hub`) `Last Mile Hub`,
max(order_placed_date) order_placed_date,
max(o.customer_id) customer_id,
max(new_vendor_id)new_vendor_id,
max(weight_buckets) weight_buckets,
max(distance_category) distance_category,
max(`Locality Zip_Code`) `Locality Zip_Code`
from ofd_vendor_alloc o
left join customer c
on o.customer_id=c.customer_id
where
delay_status!=0
group by order_number
)a;

/* old query for vendor alloc without considering last mile hub

DROP TABLE IF EXISTS driver_alloc;
Create temporary table driver_alloc as
(select * from
(
with fleet_alloc as (
select vendor_id,driver_id,driver_rating,
Vehicle_capacity_vm_weight,
sum(Vehicle_capacity_vm_weight) over(partition by vendor_id 
order by driver_rating desc,driver_id asc) cumul_capacity
from fleet)
select a.order_number,new_vendor_id,`Last Mile Hub`,order_placed_date,net_volume,driver_id,cumulative_volume,
cumul_capacity,row_number() over (partition by order_number order by cumul_capacity asc) rno
from alloc_order_id a
left join fleet_alloc f
on a.new_vendor_id=f.vendor_id
and a.cumulative_volume<=cumul_capacity
)x 
where rno=1
order by new_vendor_id,`Last Mile Hub`,order_placed_date,cumulative_volume asc
);
*/

/*
DROP view IF EXISTS cte1;
create view cte1 as 
(select * ,dense_rank() over(partition by order_placed_date, new_vendor_id, `Last Mile Hub` order by cumul_capacity asc) rnk1
from
(
with fleet_alloc as (
select vendor_id,driver_id,driver_rating,
Vehicle_capacity_vm_weight,
sum(Vehicle_capacity_vm_weight) over(partition by vendor_id 
order by driver_rating desc,driver_id asc) cumul_capacity
from fleet)
select a.order_number,new_vendor_id,`Last Mile Hub`,order_placed_date,net_volume,driver_id,cumulative_volume, cumulative_volume_1,
cumul_capacity, lag_hub, row_number() over (partition by order_number order by cumul_capacity asc) rno,`index`
from alloc_order_id a
left join fleet_alloc f
on a.new_vendor_id=f.vendor_id
and a.cumulative_volume_1<=cumul_capacity
)x 
where rno=1
order by new_vendor_id,`Last Mile Hub`,order_placed_date,cumulative_volume asc
);
*/

DROP TABLE IF EXISTS driver_alloc;
Create temporary table driver_alloc as 
(
with cte as
(select * ,dense_rank() over(partition by order_placed_date, new_vendor_id, `Last Mile Hub` order by cumul_capacity asc) rnk1
-- ,lag(cumul_capacity) over(partition by order_placed_date, new_vendor_id order by `Last Mile Hub`,cumulative_volume_1 asc) lag1
-- ,case when lag(driver_id) over(partition by order_placed_date, new_vendor_id order by `Last Mile Hub`,cumulative_volume_1 asc) 
-- != driver_id then cumulative_volume_1 else cumul_capacity end cumul_capacity_1
from
(
with fleet_alloc as (
select vendor_id,driver_id,driver_rating,
Vehicle_capacity_vm_weight,
sum(Vehicle_capacity_vm_weight) over(partition by vendor_id 
order by driver_rating desc,driver_id asc) cumul_capacity
from fleet)
select a.order_number,new_vendor_id,`Last Mile Hub`,order_placed_date,net_volume,driver_id,cumulative_volume, cumulative_volume_1,
cumul_capacity, lag_hub, row_number() over (partition by order_number order by cumul_capacity asc) rno,`index`
from alloc_order_id a
left join fleet_alloc f
on a.new_vendor_id=f.vendor_id
and a.cumulative_volume_1<=cumul_capacity
)x 
where rno=1
order by new_vendor_id,`Last Mile Hub`,order_placed_date,cumulative_volume asc
)
select d.*, coalesce(case when `index`>1 then case when rnk1=1 then (select max(driver_id) from cte t 
                                                               where t.new_vendor_id=d.new_vendor_id and t.order_placed_date=d.order_placed_date and t.`last mile hub`= d.`last mile hub` and t.rnk1=2)
                                          else driver_id end
           else driver_id end, driver_id) new_driver_id
from cte d 
);

/* Code to find the days with higher drivers
select count(distinct driver_id),new_vendor_id,`Last Mile Hub`,order_placed_date from driver_alloc
group by new_vendor_id,`Last Mile Hub`,order_placed_date
order by count(distinct driver_id) desc;
*/

/* Example of driver_allocation
select * from driver_alloc where
new_vendor_id='VEN-2000' 
-- and `Last Mile Hub`='IN-05' 
and order_placed_date='01/02/23'
-- and new_driver_id='driver046747'
 order by new_vendor_id,`Last Mile Hub`,order_placed_date,cumulative_volume asc ;
 */


/* Code to verify the row_count
select count(distinct order_number) from ofd_vendor_alloc
where delay_status!=0;
select count(*) from driver_alloc;
*/

DROP TABLE IF EXISTS ofd_final_alloc;
Create table ofd_final_alloc as
Select a.*,case when delay_status!=0 then d.new_driver_id
           else orig_driver_id end new_driver_id
from ofd_vendor_alloc a
left join driver_alloc d
on a.order_number=d.order_number;

-- select * from ofd_final_alloc;

/* Driver Allocation ends */

-- Filter the orders for top 100 zip codes by RFM
-- For the subsequent analysis, we will be using this table

DROP TABLE IF EXISTS OFD_alloc_RFM;
Create table OFD_alloc_RFM
as select o.*,c.`Locality Zip_Code` from ofd_final_alloc o
left join customer c
on o.customer_id= c.customer_id
where `Locality Zip_Code` in 
(Select distinct `Locality Zip_Code` 
from (select * from zipcodesbyRFM 
order by AvgMetric asc 
limit 100)x);

/* Company cost savings analysis */

-- Assume that on average the opportunity cost is 5% of the product price for a delay (as per market standard)
DROP view IF EXISTS cost_savings;
create  view cost_savings as (
select `Locality Zip_Code`, count(distinct order_number) order_count, 
round(avg(Opportunity_cost),2) Avg_Order_Opp_cost, 
round(avg(orig_order_delivery_price),2) Average_order_initial_delivery_cost,
round(avg(new_order_delivery_price),2) Average_order_final_delivery_cost,
round(avg(orig_order_delivery_price),2) - round(avg(new_order_delivery_price),2) Average_Order_Delivery_cost_savings
from
(
select order_number,sum(p.Price) order_price,round(sum(p.price)*0.05,2) Opportunity_cost,
avg(orig_order_delivery_price) orig_order_delivery_price,
max(new_order_delivery_price) new_order_delivery_price, 
max(`Locality Zip_Code`)`Locality Zip_Code`
from OFD_alloc_RFM o
left join products p
on o.SKU_CODE=p.SKU_CODE
where delay_status!=0
group by order_number
)x
group by `Locality Zip_Code`
);

-- Net Cost savings

select round(avg(Avg_Order_Opp_cost),2) Avg_Order_Opp_cost,
round(avg(Average_order_initial_delivery_cost),2) Average_order_initial_delivery_cost,
round(avg(Average_order_final_delivery_cost),2) Average_order_final_delivery_cost,
round(avg(Average_Order_Delivery_cost_savings),2) Average_Order_Delivery_cost_savings
from (select * from cost_savings)v;

/* Delay Analysis */

-- Graph #1

select y.`Locality Zip_Code`, round(delay_spendings/total_spending,2) Delay_Spendings_percent,
 company_avg_delays
from
(select x.*,i.total_spending from
(select sum(p.price) delay_spendings, `Locality Zip_Code`, avg(delay) delay
from OFD_alloc_RFM o
left join products p on o.SKU_Code=p.SKU_Code
where delay>0
group by `Locality Zip_Code`
)x
left join
(
select sum(p.price) total_spending, `Locality Zip_Code`
from OFD_alloc_RFM o 
left join products p on o.SKU_Code=p.SKU_Code
group by `Locality Zip_Code`
)i
on x.`Locality Zip_Code`=i.`Locality Zip_Code`
order by x.`Locality Zip_Code`
)y
left join
(
select `Locality Zip_Code`, avg(delay) company_avg_delays
from ofd_delays_company o
left join customer c
on o.customer_id=c.customer_id
where `Locality Zip_Code` in 
(Select distinct `Locality Zip_Code` 
from (select * from zipcodesbyRFM 
order by AvgMetric asc 
limit 100)x)
group by `Locality Zip_Code`
)z
on y.`Locality Zip_Code`=z.`Locality Zip_Code`;


-- Graph #2 

#Step 3:Operational Improvements Based on Delay Analysis    
-- Identify products or categories most associated with delays.
SELECT 
    Products.SKU_CODE, 
    Products.Product_Type, 
    COUNT(*) AS Delayed_Order_Count
FROM 
    Orders
JOIN
    ofd_temp oft ON orders.order_number = oft.order_number
JOIN 
    Products ON oft.SKU_CODE = Products.SKU_CODE
WHERE 
    Orders.delay > 0
GROUP BY 
    Products.SKU_CODE, Products.Product_Type
ORDER BY 
    Delayed_Order_Count DESC
limit 100;


/*Locality or Last Mile Hub Insights: If certain localities or hubs show a higher number of delays or 
longer average delays, investigate the logistical operations, routing efficiency, or hub capacity in 
these areas for potential improvements. */   

-- Graph #3

 #Analyze Delay Impact by Product Type
 SELECT 
    p.Product_Type, 
    COUNT(oft.order_number) AS Delayed_Orders,
    AVG(o.delay) AS Average_Delay
FROM 
    ofd_temp oft
JOIN 
    Orders o ON oft.order_number = o.order_number AND o.delay > 0
JOIN 
    Products p ON oft.SKU_CODE = p.SKU_CODE
GROUP BY 
    p.Product_Type
ORDER BY 
    Delayed_Orders DESC;
    
/* Should certain product types be more prone to delays, review inventory management, supplier 
reliability, and fulfillment processes related to these products. */

/* Capacity Analysis */

 #Total reduction in drivers on field per day 

WITH RankedDrivers AS (
    SELECT
        order_placed_date,
        orig_driver_id,
        DENSE_RANK() OVER(PARTITION BY order_placed_date
        ORDER BY orig_driver_id) AS orig_rank,
        new_driver_id,
        DENSE_RANK() OVER(PARTITION BY order_placed_date
        ORDER BY new_driver_id) AS new_rank
    FROM
        ofd_final_alloc
),
DriverCounts AS (
    SELECT
        order_placed_date,
        MAX(orig_rank) AS Driver_count_orig,
        MAX(new_rank) AS Driver_count_new
    FROM
        RankedDrivers
    GROUP BY
        order_placed_date
)

SELECT
    order_placed_date,
    Driver_count_orig,
    Driver_count_new
FROM
    DriverCounts
ORDER BY
    order_placed_date;













