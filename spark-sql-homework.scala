import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Задание 1: Топ-15 стран с наибольшим процентом переболевших на 31 марта 2021
println("=== Задание 1: Топ-15 стран по проценту переболевших ===")
val top15Countries = df
  .filter($"date" === "2021-03-31")
  .groupBy("iso_code", "location")
  .agg(
    max("total_cases").alias("total_cases"),
    max("population").alias("population")
  )
  .filter($"population" > 0)
  .withColumn("percent_recovered",
    round(($"total_cases" / $"population") * 100, 2)
  )
  .select("iso_code", "location", "percent_recovered")
  .orderBy(desc("percent_recovered"))
  .limit(15)

top15Countries.show(false)

// Задание 2: Топ-10 стран с максимальным количеством новых случаев за последнюю неделю марта 2021
println("\n=== Задание 2: Топ-10 стран по новым случаям (25–31 марта) ===")
val w2 = Window.partitionBy("location").orderBy(desc("new_cases"))
val task2 = df
  .filter(
    $"date" >= "2021-03-25" &&
    $"date" <= "2021-03-31" &&
    $"new_cases".isNotNull &&
    $"continent".isNotNull
  )
  .withColumn("rn", row_number().over(w2))
  .filter($"rn" === 1)
  .select(
    $"date".alias("число"),
    $"location".alias("страна"),
    $"new_cases".alias("кол-во новых случаев")
  )
  .orderBy(desc("кол-во новых случаев"))
  .limit(10)

task2.show(false)
task2.coalesce(1).write.option("header", "true")
  .mode("overwrite").csv("SparkSQL/result_task2")

// Задание 3: Изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021
println("\n=== Задание 3: Дельта новых случаев в России (24–31 марта) ===")
val w3 = Window.partitionBy("location").orderBy("date")
val task3 = df
  .filter(
    $"location" === "Russia" &&
    $"date" >= "2021-03-24" &&
    $"date" <= "2021-03-31" &&
    $"new_cases".isNotNull
  )
  .select(
    $"date".alias("число"),
    lag("new_cases", 1).over(w3).alias("кол-во новых случаев вчера"),
    $"new_cases".alias("кол-во новых случаев сегодня")
  )
  .filter($"кол-во новых случаев вчера".isNotNull && $"число" >= "2021-03-25")
  .withColumn("дельта",
    $"кол-во новых случаев сегодня" - $"кол-во новых случаев вчера"
  )
  .select("число", "кол-во новых случаев вчера", "кол-во новых случаев сегодня", "дельта")
  .orderBy("число")

task3.show(false)
task3.coalesce(1).write.option("header", "true")
  .mode("overwrite").csv("SparkSQL/result_task3")
