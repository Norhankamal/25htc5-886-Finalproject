from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, length

spark = SparkSession.builder.appName("BeautyReviewsPreprocessing").getOrCreate()

BUCKET = "25htc5-project-data-group31"

# Load all raw parts from S3
df = spark.read.json(f"s3://{BUCKET}/raw/beauty_part_*.jsonl")
print(f"Total raw records: {df.count()}")

# Drop rows missing key fields
df = df.dropna(subset=["text", "rating", "user_id", "parent_asin"])

# Remove duplicates (same user + product)
df = df.dropDuplicates(["user_id", "parent_asin"])

# Keep reviews with at least 20 characters
df = df.filter(length(col("text")) >= 20)

# Approximate token count (word count * 1.3)
df = df.withColumn("word_count", F.size(F.split(col("text"), " ")))
df = df.withColumn("approx_tokens", (col("word_count") * 1.3).cast("int"))

# Filter out very long reviews (> 512 tokens)
df = df.filter(col("approx_tokens") <= 512)

# Format as instruction-response pairs for fine-tuning
df = df.withColumn(
    "instruction",
    F.concat_ws(" ",
        F.lit("A customer rated a beauty product"),
        col("rating").cast("string"),
        F.lit("stars. Summarize their review as a helpful assistant.")
    )
)
df = df.withColumn("response", col("text"))

# Split by user_id hash to avoid data leakage
df = df.withColumn("user_hash", F.hash(col("user_id")))
train = df.filter((col("user_hash") % 10) < 8)
val   = df.filter((col("user_hash") % 10) == 8)
test  = df.filter((col("user_hash") % 10) == 9)

# Save to S3
train.select("instruction","response","rating","approx_tokens") \
     .write.mode("overwrite").json(f"s3://{BUCKET}/processed/train/")
val.select("instruction","response","rating","approx_tokens") \
   .write.mode("overwrite").json(f"s3://{BUCKET}/processed/val/")
test.select("instruction","response","rating","approx_tokens") \
    .write.mode("overwrite").json(f"s3://{BUCKET}/processed/test/")

print(f"Train: {train.count():,}, Val: {val.count():,}, Test: {test.count():,}")
spark.stop()
