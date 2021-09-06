from pyspark.sql.window import Window

from pyspark.sql import functions as f

## Analyze top 10 categories by review count

df_business_cat = businessdf.groupby("categories").agg(f.count("review_count").alias("total_review_count"))
window = Window.partitionBy(df_business_cat["categories"]).orderBy(df_business_cat['total_review_count'].desc())

df_top_categories = df_business_cat.select('*', rank().over(window).alias('rank'))

#SQL
business_cat = df_business_cat.createOrReplaceTempView("business_categories")

%sql
SELECT * FROM (
    SELECT business_categories.*,
    ROW_NUMBER() OVER(ORDER BY total_review_count DESC) rank
    FROM business_categories
  )
  WHERE rank <= 10;


# Analyze no of categories available

businessdf.createOrReplaceTempView("business")

%sql

SELECT categories, count(categories)
FROM business
GROUP BY 1
ORDER BY 2 DESC

businessdf = businessdf.filter(businessdf.categories.contains("Restaurants"))

display(businessdf)

from pyspark.sql.functions import col
df_num_restaurants = business_rest_df.select('state').groupBy('state').count()

df_num_restaurants.orderBy(col('count'), ascending = False).show()


# Analyze top Restaurants by City State

%sql
SELECT city, state, count(*) as no_of_restaurants
FROM business_restaurants
GROUP BY 1,2
ORDER BY 3 DESC

from pyspark.sql.functions import col
df_res_city_state = business_rest_df.select('city','state').groupBy('city','state').count()

df_res_city_state.orderBy(col('count'), ascending = False).show()


%sql
SELECT * FROM
(SELECT state, name, review_count,
row_number() OVER(ORDER BY review_count DESC) rank
FROM business_rest
GROUP BY 1,2,3)
WHERE rank <=10;


# Analyze top Restaurants by State Arizona

business_rest_AZ_df = business_rest_df.filter(business_rest_df['state'] == 'AZ')

%sql

SELECT * FROM
(SELECT state, name, review_count,
row_number() OVER(ORDER BY review_count DESC) rank
FROM business_rest_AZ
GROUP BY 1,2,3)
WHERE rank <=10;


from pyspark.sql import functions as f
df_best_restaurants = businessdf.join(broadcast(business_rest_AZ_df), "city")

df_best_restaurants = df_best_restaurants.filter(df_best_restaurants["city"] == 'Phoenix')

df_best_restaurants = df_best_restaurants.groupby('name','stars').agg(f.count('review_count').alias('review_count')
                                                                  
                                                                      
df_best_restaurants = df_best_restaurants.filter(df_best_restaurants['review_count'] >=10)
                                                                      
df_best_restaurnats = df_best_restaurants.filter(df_best_restaurants['stars'] >=3)

# Analyze top Restaurants by State Arizona

df_best_restaurants = businessdf.join(broadcast(business_rest_AZ_df), "city")

df_best_restaurants = df_best_restaurants.filter(df_best_restaurants["city"] == 'Phoenix')

df_restaurnats_italian = df_best_restaurants.filter(df_best_restaurants["categories"].contains('Italian'))


df_restaurnats_italian = df_restaurnats_italian.filter(df_best_restaurants['review_count'] >=5)
                                                                      
df_restaurnats_italian = df_restaurnats_italian.filter(df_best_restaurants['stars'] >=3)
