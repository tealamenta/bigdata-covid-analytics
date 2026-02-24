"""
Spark schemas for COVID-19 data.

Defines the structure of data for France and Colombia datasets.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    TimestampType
)


# ============================================================================
# FRANCE SCHEMA - Hospitalization Data
# ============================================================================

FRANCE_RAW_SCHEMA = StructType([
    StructField("dep", StringType(), nullable=False),
    StructField("sexe", StringType(), nullable=False),
    StructField("jour", StringType(), nullable=False),  # Will convert to DateType
    StructField("hosp", StringType(), nullable=True),   # Will convert to IntegerType
    StructField("rea", StringType(), nullable=True),
    StructField("HospConv", StringType(), nullable=True),
    StructField("SSR_USLD", StringType(), nullable=True),
    StructField("autres", StringType(), nullable=True),
    StructField("rad", StringType(), nullable=True),
    StructField("dc", StringType(), nullable=True),
])

FRANCE_FORMATTED_SCHEMA = StructType([
    StructField("department_code", StringType(), nullable=False),
    StructField("sex", StringType(), nullable=False),
    StructField("date", DateType(), nullable=False),
    StructField("hospitalizations", IntegerType(), nullable=True),
    StructField("icu_patients", IntegerType(), nullable=True),
    StructField("conventional_hosp", IntegerType(), nullable=True),
    StructField("ssr_usld", IntegerType(), nullable=True),
    StructField("other", IntegerType(), nullable=True),
    StructField("returned_home", IntegerType(), nullable=True),
    StructField("deaths", IntegerType(), nullable=True),
    StructField("country", StringType(), nullable=False),
    StructField("ingestion_date", DateType(), nullable=False),
])


# ============================================================================
# COLOMBIA SCHEMA - Case Data
# ============================================================================

COLOMBIA_RAW_SCHEMA = StructType([
    StructField("fecha_reporte_web", StringType(), nullable=True),
    StructField("id_de_caso", StringType(), nullable=True),
    StructField("fecha_de_notificaci_n", StringType(), nullable=True),
    StructField("departamento", StringType(), nullable=True),
    StructField("departamento_nom", StringType(), nullable=True),
    StructField("ciudad_municipio", StringType(), nullable=True),
    StructField("ciudad_municipio_nom", StringType(), nullable=True),
    StructField("edad", StringType(), nullable=True),
    StructField("sexo", StringType(), nullable=True),
    StructField("tipo_recuperacion", StringType(), nullable=True),
    StructField("estado", StringType(), nullable=True),
    StructField("fecha_muerte", StringType(), nullable=True),
    StructField("fecha_recuperado", StringType(), nullable=True),
])

COLOMBIA_FORMATTED_SCHEMA = StructType([
    StructField("report_date", DateType(), nullable=True),
    StructField("case_id", StringType(), nullable=True),
    StructField("notification_date", DateType(), nullable=True),
    StructField("department_code", StringType(), nullable=True),
    StructField("department_name", StringType(), nullable=True),
    StructField("city_code", StringType(), nullable=True),
    StructField("city_name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("sex", StringType(), nullable=True),
    StructField("recovery_type", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("death_date", DateType(), nullable=True),
    StructField("recovery_date", DateType(), nullable=True),
    StructField("country", StringType(), nullable=False),
    StructField("ingestion_date", DateType(), nullable=False),
])


# ============================================================================
# COMBINED SCHEMA - For Combination Layer
# ============================================================================

COMBINED_DAILY_SCHEMA = StructType([
    StructField("date", DateType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    StructField("region", StringType(), nullable=True),
    StructField("total_cases", IntegerType(), nullable=True),
    StructField("hospitalizations", IntegerType(), nullable=True),
    StructField("icu_patients", IntegerType(), nullable=True),
    StructField("deaths", IntegerType(), nullable=True),
    StructField("recovered", IntegerType(), nullable=True),
    StructField("hospitalization_rate", StringType(), nullable=True),
    StructField("mortality_rate", StringType(), nullable=True),
])


# ============================================================================
# COLUMN MAPPINGS
# ============================================================================

# France: Raw column name -> Formatted column name
FRANCE_COLUMN_MAPPING = {
    "dep": "department_code",
    "sexe": "sex",
    "jour": "date",
    "hosp": "hospitalizations",
    "rea": "icu_patients",
    "HospConv": "conventional_hosp",
    "SSR_USLD": "ssr_usld",
    "autres": "other",
    "rad": "returned_home",
    "dc": "deaths",
}

# Colombia: Raw column name -> Formatted column name
COLOMBIA_COLUMN_MAPPING = {
    "fecha_reporte_web": "report_date",
    "id_de_caso": "case_id",
    "fecha_de_notificaci_n": "notification_date",
    "departamento": "department_code",
    "departamento_nom": "department_name",
    "ciudad_municipio": "city_code",
    "ciudad_municipio_nom": "city_name",
    "edad": "age",
    "sexo": "sex",
    "tipo_recuperacion": "recovery_type",
    "estado": "status",
    "fecha_muerte": "death_date",
    "fecha_recuperado": "recovery_date",
}


# ============================================================================
# SEX MAPPING
# ============================================================================

# France uses 0, 1, 2
FRANCE_SEX_MAPPING = {
    "0": "ALL",
    "1": "M",
    "2": "F",
}

# Colombia uses M, F
COLOMBIA_SEX_MAPPING = {
    "M": "M",
    "F": "F",
    "m": "M",
    "f": "F",
}


# ============================================================================
# STATUS MAPPING (Colombia)
# ============================================================================

COLOMBIA_STATUS_MAPPING = {
    "Recuperado": "RECOVERED",
    "Fallecido": "DECEASED",
    "Activo": "ACTIVE",
    "N/A": "UNKNOWN",
}
