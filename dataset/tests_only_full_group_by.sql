-- Ensure ONLY_FULL_GROUP_BY is enabled for this session
SET @old_mode = @@SESSION.sql_mode;
SET SESSION sql_mode = IF(FIND_IN_SET('ONLY_FULL_GROUP_BY', @@SESSION.sql_mode)=0,
  CONCAT_WS(',', @@SESSION.sql_mode, 'ONLY_FULL_GROUP_BY'),
  @@SESSION.sql_mode);

-- Sample 1: Supplier comparative analysis Q3 vs Q4 2006 (top 5 by Q4 revenue)
WITH per_supplier AS (
  SELECT s.supplierId, s.companyName,
         SUM(CASE WHEN o.orderDate BETWEEN '2006-07-01' AND '2006-09-30' THEN od.quantity*od.unitPrice*(1-COALESCE(od.discount,0)) END) AS rev_q3,
         SUM(CASE WHEN o.orderDate BETWEEN '2006-10-01' AND '2006-12-31' THEN od.quantity*od.unitPrice*(1-COALESCE(od.discount,0)) END) AS rev_q4,
         COUNT(DISTINCT CASE WHEN o.orderDate BETWEEN '2006-07-01' AND '2006-09-30' THEN o.orderId END) AS ord_q3,
         COUNT(DISTINCT CASE WHEN o.orderDate BETWEEN '2006-10-01' AND '2006-12-31' THEN o.orderId END) AS ord_q4
  FROM Supplier s
  JOIN Product p ON p.supplierId = s.supplierId
  JOIN OrderDetail od ON od.productId = p.productId
  JOIN SalesOrder o ON o.orderId = od.orderId
  GROUP BY s.supplierId, s.companyName
), filtered AS (
  SELECT *, (rev_q4 - rev_q3) AS delta_abs,
         CASE WHEN rev_q3>0 THEN (rev_q4 - rev_q3)/rev_q3 END AS delta_pct,
         CASE WHEN rev_q3>0 THEN rev_q4/rev_q3 END AS growth_factor
  FROM per_supplier
  WHERE ord_q3 >= 3 AND ord_q4 >= 3 AND rev_q3 > 0 AND rev_q4 > 0
), totals AS (
  SELECT SUM(rev_q4) AS tot_q4_rev FROM per_supplier
)
SELECT companyName, ord_q3, ord_q4, rev_q3, rev_q4,
       delta_abs, delta_pct, growth_factor,
       rev_q4 / totals.tot_q4_rev AS q4_share,
       RANK() OVER (ORDER BY rev_q4 DESC) AS q4_rank
FROM filtered
CROSS JOIN totals
ORDER BY rev_q4 DESC
LIMIT 5;

-- Sample 2: Category monthly series with cumulative and shares (Jul 2006 - Feb 2007)
WITH cat_month AS (
  SELECT DATE_FORMAT(o.orderDate,'%Y-%m') AS ym,
         c.categoryId,
         c.categoryName,
         SUM(od.quantity*od.unitPrice*(1-COALESCE(od.discount,0))) AS month_rev
  FROM SalesOrder o
  JOIN OrderDetail od ON od.orderId = o.orderId
  JOIN Product p ON p.productId = od.productId
  JOIN Category c ON c.categoryId = p.categoryId
  WHERE o.orderDate BETWEEN '2006-07-01' AND '2007-02-28'
  GROUP BY c.categoryId, c.categoryName, DATE_FORMAT(o.orderDate,'%Y-%m')
), totals AS (
  SELECT ym, SUM(month_rev) AS tot_rev
  FROM cat_month GROUP BY ym
), cat_running AS (
  SELECT cm.*, SUM(cm.month_rev) OVER (PARTITION BY categoryId ORDER BY ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cat_cum_rev
  FROM cat_month cm
), month_running AS (
  SELECT ym, SUM(tot_rev) OVER (ORDER BY ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_all_rev
  FROM totals
)
SELECT cm.ym, cm.categoryName, cm.month_rev, cr.cat_cum_rev,
       cm.month_rev / t.tot_rev AS month_share,
       cr.cat_cum_rev / mr.cum_all_rev AS cum_share
FROM cat_month cm
JOIN cat_running cr ON cr.categoryId = cm.categoryId AND cr.ym = cm.ym
JOIN totals t ON t.ym = cm.ym
JOIN month_running mr ON mr.ym = cm.ym
ORDER BY cm.ym, cm.month_rev DESC;

-- Sample 3: Top 10 products by percent revenue change Aug 2006 vs Jan 2007
WITH prod_two AS (
  SELECT p.productId, p.productName,
         SUM(CASE WHEN DATE_FORMAT(o.orderDate,'%Y-%m')='2006-08' THEN od.quantity*od.unitPrice*(1-COALESCE(od.discount,0)) END) AS rev_aug,
         SUM(CASE WHEN DATE_FORMAT(o.orderDate,'%Y-%m')='2007-01' THEN od.quantity*od.unitPrice*(1-COALESCE(od.discount,0)) END) AS rev_jan
  FROM Product p
  JOIN OrderDetail od ON od.productId = p.productId
  JOIN SalesOrder o ON o.orderId = od.orderId
  GROUP BY p.productId, p.productName
), filtered AS (
  SELECT *, (rev_jan - rev_aug) AS delta_abs,
         CASE WHEN rev_aug>0 THEN (rev_jan - rev_aug)/rev_aug END AS delta_pct
  FROM prod_two
  WHERE COALESCE(rev_aug,0) > 0 OR COALESCE(rev_jan,0) > 0
)
SELECT productName, rev_aug, rev_jan, delta_abs, delta_pct
FROM filtered
ORDER BY (delta_pct IS NULL), delta_pct DESC, delta_abs DESC
LIMIT 10;

-- Sample 4: Shipping countries ranking with extended metrics (year 2006)
WITH per_order AS (
  SELECT o.orderId, o.shipCountry, SUM(od.quantity*od.unitPrice*(1-COALESCE(od.discount,0))) AS ord_rev, COUNT(*) AS line_cnt
  FROM SalesOrder o
  JOIN OrderDetail od ON od.orderId = o.orderId
  WHERE YEAR(o.orderDate)=2006
  GROUP BY o.orderId, o.shipCountry
), agg AS (
  SELECT shipCountry, SUM(ord_rev) AS total_rev, AVG(ord_rev) AS avg_ord_rev, COUNT(*) AS orders, SUM(line_cnt) AS total_lines
  FROM per_order
  GROUP BY shipCountry
  HAVING COUNT(*) >= 10
)
SELECT shipCountry,
       total_rev,
       avg_ord_rev,
       orders,
       total_rev/total_lines AS avg_line_rev,
       RANK() OVER (ORDER BY total_rev DESC) AS revenue_rank,
       total_rev / SUM(total_rev) OVER () AS rev_share,
       CASE
         WHEN ROW_NUMBER() OVER (ORDER BY total_rev DESC) <= CEIL(COUNT(*) OVER () * 0.25) THEN 'TOP_QUARTILE'
         WHEN ROW_NUMBER() OVER (ORDER BY total_rev DESC) <= CEIL(COUNT(*) OVER () * 0.50) THEN 'UPPER_HALF'
         ELSE 'LOWER_HALF'
       END AS segment,
       (total_rev/total_lines) / (SUM(total_rev) OVER () / SUM(total_lines) OVER ()) AS rel_avg_line_rev
FROM agg
ORDER BY total_rev DESC;

-- Sample 5: Employee timeline + territories/regions (Sep 2006 - Jan 2007)
WITH ord_base AS (
  SELECT o.orderId, o.employeeId, o.orderDate, SUM(od.quantity*od.unitPrice*(1-COALESCE(od.discount,0))) AS revenue
  FROM SalesOrder o
  JOIN OrderDetail od ON od.orderId = o.orderId
  WHERE o.orderDate BETWEEN '2006-09-01' AND '2007-01-31'
  GROUP BY o.orderId, o.employeeId, o.orderDate
), seq AS (
  SELECT ord_base.*,
         ROW_NUMBER() OVER (PARTITION BY employeeId ORDER BY orderDate) AS rn,
         AVG(revenue) OVER (PARTITION BY employeeId ORDER BY orderDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma3,
         LAG(revenue) OVER (PARTITION BY employeeId ORDER BY orderDate) AS prev_rev,
         SUM(revenue) OVER (PARTITION BY employeeId ORDER BY orderDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS emp_cum_rev,
         RANK() OVER (PARTITION BY employeeId ORDER BY revenue DESC) AS rev_rank_emp
  FROM ord_base
), day_tot AS (
  SELECT orderDate, SUM(revenue) AS day_rev
  FROM ord_base
  GROUP BY orderDate
), emp_geo AS (
  SELECT et.employeeId,
         GROUP_CONCAT(DISTINCT t.territoryDescription ORDER BY t.territoryDescription SEPARATOR ', ') AS territories,
         GROUP_CONCAT(DISTINCT r.regionDescription ORDER BY r.regionDescription SEPARATOR ', ') AS regions
  FROM EmployeeTerritory et
  JOIN Territory t ON t.territoryId = et.territoryId
  JOIN Region r ON r.regionId = t.regionId
  GROUP BY et.employeeId
), enriched AS (
  SELECT s.*, DATE_FORMAT(s.orderDate,'%Y-%m') AS ym, dt.day_rev,
         s.revenue / dt.day_rev AS day_share,
         CASE WHEN s.rn > 0 THEN s.revenue / (s.emp_cum_rev / s.rn) END AS vs_avg_so_far,
         CASE
           WHEN s.revenue < 100 THEN 'SMALL'
           WHEN s.revenue < 500 THEN 'MEDIUM'
           ELSE 'LARGE'
         END AS size_bucket,
         CASE WHEN prev_rev>0 THEN (s.revenue - prev_rev)/prev_rev END AS pct_change_prev
  FROM seq s
  JOIN day_tot dt ON dt.orderDate = s.orderDate
)
SELECT e.firstname, e.lastname, enriched.orderId, enriched.orderDate,
       enriched.revenue, rn, rev_rank_emp, ma3, emp_cum_rev, day_rev, day_share,
       vs_avg_so_far, size_bucket, pct_change_prev,
       COALESCE(eg.territories,'NO_TERRITORY') AS territories,
       COALESCE(eg.regions,'NO_REGION') AS regions
FROM enriched
JOIN Employee e ON e.employeeId = enriched.employeeId
LEFT JOIN emp_geo eg ON eg.employeeId = enriched.employeeId
ORDER BY e.lastname, e.firstname, enriched.orderDate;

-- Sample 6: Products with >=12% monthly share in 2006
WITH prod_month AS (
  SELECT DATE_FORMAT(o.orderDate,'%Y-%m') AS ym, p.productId, p.productName, c.categoryId, c.categoryName,
         SUM(od.quantity*od.unitPrice*(1-COALESCE(od.discount,0))) AS prod_rev,
         COUNT(DISTINCT o.orderId) AS orders_cnt,
         COUNT(*) AS line_cnt
  FROM SalesOrder o
  JOIN OrderDetail od ON od.orderId = o.orderId
  JOIN Product p ON p.productId = od.productId
  JOIN Category c ON c.categoryId = p.categoryId
  WHERE YEAR(o.orderDate)=2006
  GROUP BY DATE_FORMAT(o.orderDate,'%Y-%m'), p.productId, p.productName, c.categoryId, c.categoryName
), month_tot AS (
  SELECT ym, SUM(prod_rev) AS month_rev
  FROM prod_month
  GROUP BY ym
), ranked AS (
  SELECT pm.*, mt.month_rev,
         pm.prod_rev / mt.month_rev AS share,
         RANK() OVER (PARTITION BY pm.ym ORDER BY pm.prod_rev/mt.month_rev DESC) AS rank_by_share
  FROM prod_month pm
  JOIN month_tot mt ON mt.ym = pm.ym
), per_prod_seq AS (
  SELECT ranked.*,
         LAG(share) OVER (PARTITION BY productId ORDER BY ym) AS prev_share
  FROM ranked
)
SELECT ym, productName, categoryName,
       prod_rev, month_rev, share, rank_by_share,
       orders_cnt, line_cnt, prod_rev/line_cnt AS avg_line_rev,
       CASE WHEN prev_share IS NOT NULL THEN share - prev_share END AS delta_share_vs_prev_month
FROM per_prod_seq
WHERE share >= 0.12
ORDER BY ym, share DESC;

-- Sample 7: Eligible customers monthly + demographics (Jul 2006 - Feb 2007)
WITH ord AS (
  SELECT o.orderId,
         o.custId,
         DATE_FORMAT(o.orderDate,'%Y-%m') AS ym,
         SUM(od.quantity*od.unitPrice*(1-COALESCE(od.discount,0))) AS rev
  FROM SalesOrder o
  JOIN OrderDetail od ON od.orderId = o.orderId
  WHERE o.orderDate BETWEEN '2006-07-01' AND '2007-02-28'
  GROUP BY o.orderId, o.custId, DATE_FORMAT(o.orderDate,'%Y-%m')
), counts AS (
  SELECT custId, COUNT(orderId) AS ord_cnt FROM ord GROUP BY custId
), eligible AS (
  SELECT custId FROM counts WHERE ord_cnt >= 6
), cust_month AS (
  SELECT o.custId, o.ym, SUM(rev) As month_rev
  FROM ord o JOIN eligible e ON e.custId = o.custId
  GROUP BY o.custId, o.ym
), cust_enriched AS (
  SELECT cust_month.*, SUM(month_rev) OVER (PARTITION BY custId ORDER BY ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cust_cum_rev
  FROM cust_month
), month_tot AS (
  SELECT ym, SUM(month_rev) AS tot_month_rev FROM cust_month GROUP BY ym
), cum_tot AS (
  SELECT ym, SUM(tot_month_rev) OVER (ORDER BY ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_all_rev FROM month_tot
), ranked_month AS (
  SELECT ce.*, RANK() OVER (PARTITION BY ym ORDER BY month_rev DESC) AS rk
  FROM cust_enriched ce
), cust_demo AS (
  SELECT ccd.custId,
         GROUP_CONCAT(DISTINCT cd.customerDesc ORDER BY cd.customerDesc SEPARATOR ', ') AS segments
  FROM CustCustomerDemographics ccd
  JOIN CustomerDemographics cd ON cd.customerTypeId = ccd.customerTypeId
  GROUP BY ccd.custId
)
SELECT c.companyName, rm.ym, rm.month_rev, rm.cust_cum_rev, rk AS month_rank,
       rm.cust_cum_rev / ct.cum_all_rev AS cust_cum_share,
       COALESCE(d.segments,'NO_SEGMENT') AS segments,
       EXISTS(SELECT 1 FROM CustCustomerDemographics x WHERE x.custId = rm.custId) AS has_demographics
FROM ranked_month rm
JOIN Customer c ON c.custId = rm.custId
JOIN cum_tot ct ON ct.ym = rm.ym
LEFT JOIN cust_demo d ON d.custId = rm.custId
ORDER BY c.companyName, rm.ym;

-- Sample 8: Shippers with avg ship time > 6 and revenue above median (year 2006)
WITH ord_ship AS (
  SELECT o.orderId, o.shipVia, DATEDIFF(o.shippedDate,o.orderDate) AS ship_days, SUM(od.quantity*od.unitPrice*(1-COALESCE(od.discount,0))) AS ord_rev
  FROM SalesOrder o
  JOIN OrderDetail od ON od.orderId = o.orderId
  WHERE YEAR(o.orderDate)=2006
  GROUP BY o.orderId, o.shipVia, o.orderDate, o.shippedDate
), ship_metrics AS (
  SELECT s.shipperId, s.companyName, AVG(os.ship_days) AS avg_ship_days, SUM(os.ord_rev) AS total_rev
  FROM ord_ship os
  JOIN Shipper s ON s.shipperId = os.shipVia
  GROUP BY s.shipperId, s.companyName
), ordered AS (
  SELECT total_rev, ROW_NUMBER() OVER (ORDER BY total_rev) AS rn, COUNT(*) OVER () AS cnt
  FROM ship_metrics
), median_calc AS (
  SELECT AVG(total_rev) AS median_rev
  FROM ordered
  WHERE rn IN (FLOOR((cnt+1)/2), CEIL((cnt+1)/2))
)
SELECT companyName, total_rev, avg_ship_days, (total_rev - mc.median_rev) AS delta_vs_median
FROM ship_metrics sm
CROSS JOIN median_calc mc
WHERE sm.avg_ship_days > 6 AND sm.total_rev > mc.median_rev
ORDER BY sm.total_rev DESC;

-- Sample 9: Extended seasonality panel with median and stddev (Jul 2006 - Feb 2007)
WITH per_order AS (
  SELECT o.orderId,
         DATE_FORMAT(o.orderDate,'%Y-%m') AS ym,
         SUM(od.quantity*od.unitPrice*(1-COALESCE(od.discount,0))) AS ord_rev
  FROM SalesOrder o
  JOIN OrderDetail od ON od.orderId = o.orderId
  WHERE o.orderDate BETWEEN '2006-07-01' AND '2007-02-28'
  GROUP BY o.orderId, DATE_FORMAT(o.orderDate,'%Y-%m')
), month_stats AS (
  SELECT ym, COUNT(*) AS orders, SUM(ord_rev) AS revenue, AVG(ord_rev) AS avg_rev, STDDEV_POP(ord_rev) AS std_rev
  FROM per_order GROUP BY ym
), ordered AS (
  SELECT ym, ord_rev, ROW_NUMBER() OVER (PARTITION BY ym ORDER BY ord_rev) AS rn, COUNT(*) OVER (PARTITION BY ym) AS cnt
  FROM per_order
), med AS (
  SELECT ym, AVG(ord_rev) AS median_rev
  FROM ordered
  WHERE rn IN (FLOOR((cnt+1)/2), CEIL((cnt+1)/2))
  GROUP BY ym
)
SELECT ms.ym, orders, revenue, avg_rev, med.median_rev, std_rev,
       LAG(revenue) OVER (ORDER BY ms.ym) AS prev_revenue,
       CASE WHEN LAG(revenue) OVER (ORDER BY ms.ym) > 0 THEN (revenue - LAG(revenue) OVER (ORDER BY ms.ym))/LAG(revenue) OVER (ORDER BY ms.ym) END AS pct_change
FROM month_stats ms
JOIN med ON med.ym = ms.ym
ORDER BY ms.ym;

-- Sample 10: Top 3 products by revenue per category (year 2006)
WITH cat_prod AS (
  SELECT c.categoryId, c.categoryName, p.productId, p.productName, SUM(od.quantity*od.unitPrice*(1-COALESCE(od.discount,0))) AS prod_rev
  FROM SalesOrder o
  JOIN OrderDetail od ON od.orderId = o.orderId
  JOIN Product p ON p.productId = od.productId
  JOIN Category c ON c.categoryId = p.categoryId
  WHERE YEAR(o.orderDate)=2006
  GROUP BY c.categoryId, c.categoryName, p.productId, p.productName
), cat_tot AS (
  SELECT categoryId, SUM(prod_rev) AS cat_rev FROM cat_prod GROUP BY categoryId
), ranked AS (
  SELECT cp.*, cp.prod_rev/ct.cat_rev AS share_cat, ROW_NUMBER() OVER (PARTITION BY cp.categoryId ORDER BY cp.prod_rev DESC) AS rk
  FROM cat_prod cp JOIN cat_tot ct ON ct.categoryId = cp.categoryId
)
SELECT categoryName, productName, prod_rev, share_cat, rk
FROM ranked
WHERE rk <= 3
ORDER BY categoryName, rk;

-- Restore previous sql_mode
SET SESSION sql_mode = @old_mode;
