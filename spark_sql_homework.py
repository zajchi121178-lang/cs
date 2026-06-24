from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import glob
import csv

# 1. Инициализация Spark
spark = SparkSession.builder.appName("CovidAnalysis").getOrCreate()

# 2. Поиск файла
data_folder = "data"
csv_files = glob.glob(os.path.join(data_folder, "*.csv"))

if not csv_files:
    print(f"ОШИБКА: В папке '{data_folder}' нет файлов .csv")
    spark.stop()
    exit(1)

input_file_path = csv_files[0]
print(f"Найден файл данных: {input_file_path}")

output_dir = "results"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

try:
    df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(input_file_path)
    print("Данные загружены в Spark DataFrame.")
except Exception as e:
    print(f"Ошибка чтения данных: {e}")
    spark.stop()
    exit(1)

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ЗАПИСИ ---
def save_to_csv(rows, filename, headers):
    path = os.path.join(output_dir, filename)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
    print(f"Задание сохранено: {path}")

# --- ЗАДАНИЕ 1 ---
task1 = (
    df.filter(F.col('date') == '2021-03-31')
      .filter(F.col('population').isNotNull() & F.col('total_cases').isNotNull())
      .select(
          F.col('iso_code'),
          F.col('location').alias('Страна'),
          F.round((F.col('total_cases') / F.col('population')) * 100, 2).alias('Процент переболевших')
      )
      .orderBy(F.col('Процент переболевших').desc())
      .limit(15)
)
t1_data = task1.collect()
t1_rows = [list(row) for row in t1_data]
save_to_csv(t1_rows, "task1_top15_countries.csv", ["iso_code", "Страна", "Процент переболевших"])

# --- ЗАДАНИЕ 2 (ИСПРАВЛЕНО: убрали фильтр по continent) ---
w2 = Window.partitionBy('location').orderBy(F.col('new_cases').desc())

task2 = (
    df.filter((F.col('date') >= '2021-03-25') & (F.col('date') <= '2021-03-31'))
      .filter(F.col('new_cases').isNotNull())  # <-- Убрали & F.col('continent').isNotNull()
      .withColumn('rn', F.row_number().over(w2))
      .filter(F.col('rn') == 1)
      .select(
          F.col('date').alias('число'),
          F.col('location').alias('страна'),
          F.col('new_cases').alias('кол-во новых случаев')
      )
      .orderBy(F.col('кол-во новых случаев').desc())
      .limit(10)
)
t2_data = task2.collect()
t2_rows = [list(row) for row in t2_data]
save_to_csv(t2_rows, "task2_top10_new_cases.csv", ["число", "страна", "кол-во новых случаев"])

# --- ЗАДАНИЕ 3 ---
w3 = Window.partitionBy('location').orderBy('date')

task3 = (
    df.filter((F.col('location') == 'Russia') & (F.col('date') >= '2021-03-24') & (F.col('date') <= '2021-03-31'))
      .filter(F.col('new_cases').isNotNull())
      .select(
          F.col('date').alias('число'),
          F.lag('new_cases', 1).over(w3).alias('кол-во новых случаев вчера'),
          F.col('new_cases').alias('кол-во новых случаев сегодня')
      )
      .filter(F.col('кол-во новых случаев вчера').isNotNull())
      .withColumn('дельта', F.col('кол-во новых случаев сегодня') - F.col('кол-во новых случаев вчера'))
      .select('число', 'кол-во новых случаев вчера', 'кол-во новых случаев сегодня', 'дельта')
      .orderBy('число')
)
t3_data = task3.collect()
t3_rows = [list(row) for row in t3_data]
save_to_csv(t3_rows, "task3_russia_delta.csv", ["число", "кол-во новых случаев вчера", "кол-во новых случаев сегодня", "дельта"])

spark.stop()
print("Готово! Все файлы сохранены.")
