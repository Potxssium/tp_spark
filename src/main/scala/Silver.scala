import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._
        

object Silver {

    //Fonction d'ajout de la date de traitement dans un fichier CSV
    def addProcessingDateAndSave(filename: String, inputPath: String, outputPath: String, spark: SparkSession, dfOpt: Option[org.apache.spark.sql.DataFrame] = None): String = {

        val df = dfOpt.getOrElse {
            spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .csv(s"$inputPath/$filename")
        }

        val dfWithDate = df.withColumn("date_traitement", current_timestamp())
        
        dfWithDate.write
            .mode("overwrite")
            .option("header", "true")
            .option("delimiter", ";")
            // Je me suis un peu amusé à tester des options pour passer les noms de fichiers
            .parquet(s"$outputPath/${filename.replace(".csv", "")}")
        
        s"Traitement terminé: $outputPath/$filename"
    }

    // Fonction d'ajout du nom de la région en fonction du code académie
    def addRegionName(inputPath: String, baccalaureatFileName: String, academiesFileName: String, outputPath: String, spark: SparkSession): String = {
        
        val baccalaureatDf = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ";")
            .csv(s"$inputPath/$baccalaureatFileName")
        

        val academiesDf = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ";")
            .csv(s"$inputPath/$academiesFileName")

        // Jointure sur la colonne `Code académie` pour ajouter le nom de la région
        val enrichedDf = baccalaureatDf
        .join(
            academiesDf.select(
            col("code_academie").as("Code académie"),
            col("libelle_region_2016").as("Région")
            ),
            Seq("Code académie"),
            "left"
        )

        addProcessingDateAndSave(baccalaureatFileName, inputPath, outputPath, spark, Some(enrichedDf))
        addProcessingDateAndSave(academiesFileName, inputPath, outputPath, spark, Some(academiesDf))
        addProcessingDateAndSave("fr-esr-implantations_etablissements_d_enseignement_superieur_publics.csv", inputPath, outputPath, spark)
    
        s"Ajout de date nom de région terminé: $outputPath"    
    }

    // Note : Le main attend deux arguments : le chemin d'accès aux fichiers d'entrée et le chemin de sortie
    // J'ai compilé et lancé le main depuis le fichier Run_Spark.sh situé à la racine du projet
    def main(args: Array[String]): Unit = {
        if (args.length != 2) {
            println("Usage: Silver <inputPath> <outputPath>")
            sys.exit(1)
        }

        val spark = SparkSessionBuilder.SparkSessionBuilder()

        val inputPath = args(0)
        val outputPath = args(1)

        val resultMessage = addRegionName(inputPath, "fr-en-baccalaureat-par-academie.csv", "fr-en-contour-academies-2020.csv", outputPath, spark)
        println(resultMessage)
    }
}