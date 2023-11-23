from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum, desc, when, lower, row_number
from dotenv import load_dotenv
import os

load_dotenv()
postgres_user = os.getenv('user')
postgres_password = os.getenv('password')
postgres_url = os.getenv('url')

# Create a Spark session
spark = SparkSession.builder \
    .appName("PySparkExample") \
    .config("spark.jars", "/home/user/Downloads/postgresql-42.7.0.jar") \
    .config("spark.kubernetes.container.image", "spark:our-own-apache-spark-kb8") \
    .getOrCreate()

# Define PostgreSQL connection details
postgres_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

# Read data from PostgreSQL into DataFrames
city_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.city").options(**postgres_properties).load()
address_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.address").options(**postgres_properties).load()
store_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.store").options(**postgres_properties).load()
staff_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.staff").options(**postgres_properties).load()
rental_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.rental").options(**postgres_properties).load()
payment_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.payment").options(**postgres_properties).load()
inventory_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.inventory").options(**postgres_properties).load()
film_category_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.film_category").options(**postgres_properties).load()
film_actor_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.film_actor").options(**postgres_properties).load()
film_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.film").options(**postgres_properties).load()
category_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.category").options(**postgres_properties).load()
actor_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.actor").options(**postgres_properties).load()
customer_df = spark.read.format("jdbc").option("url", postgres_url).option("dbtable", "public.customer").options(**postgres_properties).load()

# 1. Display the number of films in each category, sorted in descending order.
films_per_category = film_category_df.join(category_df, film_category_df.category_id == category_df.category_id)\
    .join(film_df, film_category_df.film_id == film_df.film_id)\
    .groupBy("name").count().orderBy(desc("count"))
films_per_category.show()

# 2. Display the top 10 actors whose films were rented the most, sorted in descending order.
top_actors = film_actor_df.join(film_df, film_actor_df.film_id == film_df.film_id)\
    .join(inventory_df, film_df.film_id == inventory_df.film_id)\
    .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id)\
    .join(actor_df, film_actor_df.actor_id == actor_df.actor_id)\
    .groupBy('first_name', 'last_name').count().orderBy(desc("count")).limit(10)
top_actors.show()

# 3. Display the category of films that had the highest total payment amount.
category_with_highest_payment = category_df.alias('c')\
    .join(film_category_df.alias("fc"), col('c.category_id') == col("fc.category_id"))\
    .join(film_df.alias("f"), col("fc.film_id") == col("f.film_id"))\
    .join(inventory_df.alias("i"), col("f.film_id") == col("i.film_id"))\
    .join(rental_df.alias("r"), col("i.inventory_id") == col("r.inventory_id"))\
    .join(payment_df.alias("p"), col("r.rental_id") == col("p.rental_id"))\
    .groupBy("c.name")\
    .agg(sum("p.amount").alias("total_amount"))\
    .orderBy(desc("total_amount"))\
    .limit(5)
category_with_highest_payment.show()

# 4. Display the titles of films that are not in inventory.
films_not_in_inventory = film_df.join(inventory_df, film_df.film_id == inventory_df.film_id, "left_outer")\
    .filter("inventory_id IS NULL").select("title")
films_not_in_inventory.show()

# 5. Display the top 3 actors who appeared the most in films in the "Children" category.
top_actors_children = category_df.filter("name = 'Children'")\
    .join(film_category_df, film_category_df.category_id == category_df.category_id)\
    .join(film_df, film_df.film_id == film_category_df.film_id)\
    .join(film_actor_df, film_actor_df.film_id == film_df.film_id)\
    .join(actor_df, actor_df.actor_id == film_actor_df.actor_id)\
    .groupBy(col('first_name').alias('actor_first_name'), col('last_name').alias('actor_last_name'))\
    .count().orderBy(desc("count")).limit(3)
top_actors_children.show()

# 6. Display cities with the number of active and inactive customers, sorted by the count of inactive customers.
cities_active_inact = (
    customer_df
    .join(address_df, customer_df.address_id == address_df.address_id)
    .join(city_df, address_df.city_id == city_df.city_id)
    .groupBy("city")
    .agg(
        sum(when(col("active") == 1, 1).otherwise(0)).alias("active_customers"),
        sum(when(col("active") == 0, 1).otherwise(0)).alias("inactive_customers")
    )
    .orderBy(desc("inactive_customers"))
)
cities_active_inact.show()

# 7. Display the film category with the highest total rental hours in cities.
# Also, do the same for cities containing the symbol "-".
# Join dataframes to get relevant information
film_cat_total_hours = film_category_df \
    .join(film_df, film_category_df.film_id == film_df.film_id) \
    .join(inventory_df, film_df.film_id == inventory_df.film_id) \
    .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id) \
    .join(customer_df, rental_df.customer_id == customer_df.customer_id) \
    .join(address_df, customer_df.address_id == address_df.address_id)

# Group by category and city, summing rental durations
film_cat_total_hours = film_cat_total_hours.groupBy("city_id", "category_id").agg(sum("rental_duration").alias("total_rental_hours"))

# Join with city_df, filter cities, and add a row number for ranking
film_cat_total_hours = film_cat_total_hours.join(city_df, film_cat_total_hours.city_id == city_df.city_id) \
    .where((city_df.city_id.like("a%")) | (city_df.city.like("%-%"))) \
    .withColumn('num', row_number().over(Window.partitionBy(city_df.city_id).orderBy(desc('total_rental_hours'))))

# Select relevant columns and show the result
film_cat_total_hours.select("city", "category_id", "total_rental_hours", "num").show()

# Stop the Spark session
spark.stop()