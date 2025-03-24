-- Fact Table: fact_top_selling_products
SELECT 
    oi.product_id,
    pt.product_category_name_english AS product_category,
    COUNT(oi.order_id) AS total_sales,
    SUM(oi.price) AS total_revenue
FROM `dsai-module-2-project.olist.order_items` oi

JOIN `dsai-module-2-project.olist.products` p ON oi.product_id = p.product_id

LEFT JOIN `dsai-module-2-project.olist.product_translation` pt ON p.product_category_name = pt.product_category_name

GROUP BY oi.product_id, product_category
ORDER BY total_sales DESC