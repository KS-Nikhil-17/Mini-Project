--Find the hod of student
SELECT (s.first_name ||' '|| s.last_name)AS name , c.teacher AS HOD
FROM students s JOIN class c 
ON s.class_id=c.class_id AND s.enrollment_year=c.class_year;

--Find the category of each item in order

SELECT t1.order_id,t3.name,EXTRACT (MONTH FROM t2.order_date) AS order_date
,t1.amount FROM order_details t1
JOIN orders t2 ON t1.order_id=t2.order_id
JOIN users t3 ON t2.user_id=t3.user_id;


--
SELECT t1.order_id , t2.category 
FROM order_details t1
JOIN category t2 ON t1.category_id=t2.category_id;

-- Find the orders placed from Pune
SELECT t3.name , t1.order_id , t1.amount
FROM order_details t1
JOIN orders t2 ON t1.order_id=t2.order_id
JOIN users t3 ON t2.user_id=t3.user_id
WHERE t3.city='Pune';

-- Find all Profitable Orders

SELECT t1.order_id , SUM(t2.profit) AS profit FROM orders t1
JOIN order_details t2 ON t1.order_id=t2.order_id
GROUP BY t1.order_id
HAVING SUM(t2.profit)>0;






















