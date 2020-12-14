------------------------- Analytical Procedures --------------------
-- About Product --
-- 1. What are the 10 best-selling product categories ranked by number of orders?
SELECT C.product_category_name, count(M.order_id) AS number_of_order,
RANK() OVER (ORDER BY count(M.order_id) DESC) AS category_rank 
FROM category AS C 
LEFT JOIN product_detail AS PD on C.category_id = PD.category_id 
LEFT JOIN merge AS M on M.pd_id = PD.pd_id 
GROUP BY C.product_category_name
LIMIT 10

-- 2. What are the top 10 popular product design specifications?
SELECT Y.product_length_cm, Y.product_width_cm, Y.product_height_cm, 
count(M.order_id) AS number_of_order,
ROW_NUMBER () OVER (ORDER BY count(M.order_id) DESC) AS size_rank 
FROM size as Y
LEFT JOIN product_detail AS PD on Y.size_id = PD.size_id 
LEFT JOIN merge AS M on M.pd_id = PD.pd_id 
GROUP BY Y.size_id
LIMIT 10


-- About Customer --
-- 3. Customers from which geographical regions make the most online purchase?
SELECT L.city, L.state, count(M.order_id) AS number_of_order,
RANK() OVER (ORDER BY count(M.order_id) DESC) AS customer_rank 
FROM location AS L 
LEFT JOIN customer_location AS CL on L.zipcode = CL.location_id
LEFT JOIN customer AS C on CL.customer_id = C.customer_id
LEFT JOIN merge AS M on C.customer_id = M.customer_id 
GROUP BY L.city, L.state
LIMIT 10

-- About Delivery --
-- 4. Which month(s) throughout the year are most likely to experience delivery lag?
SELECT EXTRACT(MONTH FROM O.order_purchase_timestamp) AS month,
AVG(DATE_PART('day', D.order_delivered_customer_date - O.order_purchase_timestamp)) AS avg_delivery_time
FROM orders AS O LEFT JOIN delivery AS D ON O.order_id = D.delivery_id
GROUP BY EXTRACT(MONTH FROM O.order_purchase_timestamp)

-- 5. What is the distribution of freight values which are less than or equal to 100?
SELECT count(M.order_id) AS number_of_order, P.freight_value 
FROM price as P LEFT JOIN merge as M on P.price_id = M.price_id
WHERE freight_value <= 100
GROUP BY P.freight_value
ORDER BY P.freight_value DESC 

SELECT price_id, freight_value 
FROM price
WHERE freight_value <= 100
ORDER BY freight_value DESC

-- About Price & Payment --
-- 6. What are the top 10 orders with the highest price value?
SELECT M.order_id, PD.product_id, C.product_category_name, Z.price 
FROM merge as M
LEFT JOIN product_detail AS PD on M.pd_id = PD.pd_id
LEFT JOIN category AS C on PD.category_id = C.category_id
LEFT JOIN price AS Z on M.price_id = Z.price_id
ORDER BY Z.price DESC
limit 10

-- 7. What is the customer's favorite payment method?
SELECT Y.payment_type, count (M.order_id) as number_of_order
FROM payment as Y, merge as M
GROUP BY Y.payment_type
ORDER BY count (M.order_id) DESC

-- About Review & Comment
-- 8. What are the top 10 products with most comments?
select PD.product_id, count(F.comment_id) AS number_of_comments,
RANK () OVER (ORDER BY count(F.comment_id) DESC) AS comment_rank
FROM merge AS M 
LEFT JOIN product_detail AS PD on M.pd_id = PD.pd_id
LEFT JOIN feedback AS F on M.feedback_id = F.feedback_id
group by PD.product_id
order by comment_rank
limit 10

-- 9. What are the 10 product categories with lowest review scores?
SELECT C.product_category_name, AVG(R.review_score) AS avg_score
FROM merge as M
LEFT JOIN product_detail AS PD on M.pd_id = PD.pd_id
LEFT JOIN category AS C on PD.category_id = C.category_id
LEFT JOIN feedback as F on M.feedback_id = F.feedback_id
LEFT JOIN review as R on F.review_id = R.review_id
GROUP BY C.product_category_name
ORDER BY avg_score
LIMIT 10

-- 10. What is the relationship between comment lengths and review scores?
-- Question: Customers are more likely to give lower scores when writing long comments?
SELECT LENGTH (X.review_comment_message), AVG(R.review_score) AS avg_score
FROM comment as X 
LEFT JOIN feedback AS F on X.comment_id = F.comment_id
LEFT JOIN review AS R on F.review_id = R.review_id
GROUP BY LENGTH (X.review_comment_message)
