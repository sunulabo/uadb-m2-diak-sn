import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sha2,
    concat,
    lit,
    from_json,
    to_json,
    struct,
    window,
    count,
    current_timestamp,
    when
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# ============================================================
# CONFIGURATION
# ============================================================

BROKERS = "kafka:9092"

SALT = os.environ.get(
    "DIAK_SECRET_SALT",
    "default_salt"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("Diak_SN_Streaming")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("==========================================")
print("   DIAK-SN SPARK STREAMING DEMARRE")
print("==========================================")
print("Kafka :", BROKERS)


# ============================================================
# SCHEMA DES CAS MEDICAUX
# ============================================================

cas_schema = StructType([
    StructField("cas_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("district", StringType(), True),
    StructField("pathologie", StringType(), True),
    StructField("age_tranche", StringType(), True),
    StructField("sexe", StringType(), True),
    StructField("gravite", StringType(), True),
    StructField("timestamp", StringType(), True)
])


# ============================================================
# LECTURE KAFKA
# TOPIC : diak_cas_raw
# ============================================================

cas_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BROKERS)
    .option("subscribe", "diak_cas_raw")
    .option("startingOffsets", "latest")
    .load()
)


# ============================================================
# CONVERSION JSON
# ============================================================

cas_df = (
    cas_df
    .select(
        from_json(
            col("value").cast("string"),
            cas_schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# ANONYMISATION
# ============================================================

cas_df = (
    cas_df
    .withColumn(
        "cas_id_secure",
        sha2(
            concat(
                col("patient_id"),
                lit(SALT)
            ),
            256
        )
    )
    .drop("patient_id")
)


# ============================================================
# TIMESTAMP EVENEMENT
# ============================================================

cas_df = (
    cas_df
    .withColumn(
        "event_ts",
        current_timestamp()
    )
)


# ============================================================
# DETECTION EPIDEMIQUE
#
# Fenêtre :
#   7 jours
#
# Seuil :
#   > 100  => CRITIQUE
#   > 50   => ALERTE
#   sinon  => NORMAL
# ============================================================

epidemie_df = (
    cas_df
    .withWatermark(
        "event_ts",
        "1 day"
    )
    .groupBy(
        window(
            "event_ts",
            "7 days",
            "1 day"
        ),
        "district",
        "pathologie"
    )
    .agg(
        count("cas_id").alias("nb_cas_7j")
    )
    .withColumn(
        "alerte_epidemie",
        when(
            col("nb_cas_7j") > 100,
            lit("CRITIQUE")
        )
        .when(
            col("nb_cas_7j") > 50,
            lit("ALERTE")
        )
        .otherwise(
            lit("NORMAL")
        )
    )
)


# ============================================================
# PREPARATION DU JSON POUR NIFI
# ============================================================

alerts_df = (
    epidemie_df
    .select(
        to_json(
            struct(
                col("district"),
                col("pathologie"),
                col("nb_cas_7j"),
                col("alerte_epidemie"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end")
            )
        ).alias("value")
    )
)


# ============================================================
# ECRITURE DANS KAFKA
#
# Spark NE TOUCHE PAS HBASE.
#
# Spark produit uniquement :
#
#       diak_alerts
#
# Ensuite :
#
#       Kafka → NiFi → HBase
# ============================================================

query_alerts = (
    alerts_df
    .writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        BROKERS
    )
    .option(
        "topic",
        "diak_alerts"
    )
    .option(
        "checkpointLocation",
        "/tmp/diak_epidemie_ckpt"
    )
    .outputMode("update")
    .start()
)


print("==========================================")
print("Spark écoute le topic : diak_cas_raw")
print("Spark produit le topic : diak_alerts")
print("NiFi sera responsable de HBase")
print("==========================================")


# ============================================================
# ATTENTE
# ============================================================

query_alerts.awaitTermination()