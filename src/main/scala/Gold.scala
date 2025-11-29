import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}

object Gold {

    //Paramètres de connexion à la bd (en dur pour le tp)
    private val dbUrl = "jdbc:postgresql://172.26.71.208:5432/info"
    // private val dbTable = "test_etablissements_par_type_region"
    private val dbUser = "E219130K"
    private val dbPassword = "E219130K"


    //Enregister un df en tant que table dans la BD
    def saveToPostgres(df: DataFrame, dbTable: String): String = {

        //Config de la connexion
        val connectionProperties = new Properties()
        connectionProperties.put("user", dbUser)
        connectionProperties.put("password", dbPassword)
        connectionProperties.put("driver", "org.postgresql.Driver")
        connectionProperties.put("batchsize", "1000")
        

        val numParallelWrites = 8
        
        //Ecriture du df dans la bd
        df.repartition(numParallelWrites)
        .write
        .mode(SaveMode.Overwrite) 
        .jdbc(dbUrl, dbTable, connectionProperties)

        s"DataFrame saved to PostgreSQL table: $dbTable"}

    // Fonction pour lire fichier parquet
    def readParquet(inputPath: String, filename: String, spark: SparkSession): org.apache.spark.sql.DataFrame = {
        val df = spark.read
            .parquet(s"$inputPath/$filename")
        df
    }

    // Fonction pour compter le nombre d'établissements supérieurs par type et par région
    def countEtablissementsParTypeRegion(inputPath: String, spark: SparkSession): DataFrame = {
        
        // Lecture du fichier parquet des établissements d'enseignement supérieur
        val etablissementsDf = readParquet(inputPath, "fr-esr-implantations_etablissements_d_enseignement_superieur_publics", spark)

        // Agrégation : nombre d'établissements par académie, région et type d'établissement siège
        val resultDf = etablissementsDf
            .groupBy(
                col("Académie").as("libelle_academie"),
                col("Code académie").as("code_academie"),
                col("Code région").as("code_region_2016"),
                col("type de l'établissement siège").as("type_etablissement_siege")
            )
            .agg(
                count("*").as("nombre_etablissement")
            )
            .orderBy(
                col("code_region_2016"),
                col("code_academie"),
                col("type_etablissement_siege")
            )

        resultDf
    }

    // Fonction pour calculer le pourcentage de réussite au baccalauréat général par académie, année et genre
    def pourcentageReussiteBacGeneral(inputPath: String, spark: SparkSession): DataFrame = {
        
        // Lecture du fichier parquet du baccalauréat (enrichi avec la région)
        val baccalaureatDf = readParquet(inputPath, "fr-en-baccalaureat-par-academie", spark)

        // Filtrer uniquement sur le baccalauréat général
        val bacGeneralDf = baccalaureatDf
            .filter(col("Voie") === "BAC GENERAL")

        // Agrégation par académie, année et genre
        val statsParAcademieGenreDf = bacGeneralDf
            .groupBy(
                col("Académie").as("libelle_academie"),
                col("Code académie").as("code_academie"),
                col("Région").as("code_region_2016"),
                col("Session").as("session"),
                col("Sexe").as("genre")
            )
            .agg(
                sum("Nombre de présents").as("nb_presents"),
                sum("Nombre d'admis totaux").as("nb_admis")
            )
            .withColumn("pourcentage_reussite", 
                round((col("nb_admis") / col("nb_presents")) * 100, 2)
            )

        // Calcul du pourcentage de réussite par académie et année (tous genres confondus)
        val statsParAcademieDf = bacGeneralDf
            .groupBy(
                col("Code académie"),
                col("Session")
            )
            .agg(
                sum("Nombre de présents").as("nb_presents_academie"),
                sum("Nombre d'admis totaux").as("nb_admis_academie")
            )
            .withColumn("pourcentage_reussite_academie_tous_genres", 
                round((col("nb_admis_academie") / col("nb_presents_academie")) * 100, 2)
            )
            .select(
                col("Code académie").as("code_aca_join"),
                col("Session").as("session_join"),
                col("pourcentage_reussite_academie_tous_genres")
            )

        // Calcul du pourcentage de réussite national par année et genre
        val statsNationalGenreDf = bacGeneralDf
            .groupBy(
                col("Session"),
                col("Sexe")
            )
            .agg(
                sum("Nombre de présents").as("nb_presents_national_genre"),
                sum("Nombre d'admis totaux").as("nb_admis_national_genre")
            )
            .withColumn("pourcentage_reussite_national_genre", 
                round((col("nb_admis_national_genre") / col("nb_presents_national_genre")) * 100, 2)
            )
            .select(
                col("Session").as("session_nat_genre"),
                col("Sexe").as("genre_nat"),
                col("pourcentage_reussite_national_genre")
            )

        // Calcul du %age de réussite national par année
        val statsNationalDf = bacGeneralDf
            .groupBy(col("Session"))
            .agg(
                sum("Nombre de présents").as("nb_presents_national"),
                sum("Nombre d'admis totaux").as("nb_admis_national")
            )
            .withColumn("pourcentage_reussite_national", 
                round((col("nb_admis_national") / col("nb_presents_national")) * 100, 2)
            )
            .select(
                col("Session").as("session_nat"),
                col("pourcentage_reussite_national")
            )

        // Jointure des différentes statistiques
        val resultDf = statsParAcademieGenreDf
            // Jointure avec stats académie tous genres
            .join(statsParAcademieDf, 
                col("code_academie") === col("code_aca_join") && col("session") === col("session_join"),
                "left"
            )
            // Jointure avec stats national par genre
            .join(statsNationalGenreDf,
                col("session") === col("session_nat_genre") && col("genre") === col("genre_nat"),
                "left"
            )
            // Jointure avec stats national tous genres
            .join(statsNationalDf,
                col("session") === col("session_nat"),
                "left"
            )
            .select(
                col("libelle_academie"),
                col("code_academie"),
                col("code_region_2016"),
                col("session"),
                col("genre"),
                col("pourcentage_reussite"),
                col("pourcentage_reussite_academie_tous_genres"),
                col("pourcentage_reussite_national_genre"),
                col("pourcentage_reussite_national")
            )
            .orderBy(
                col("session"),
                col("code_academie"),
                col("genre")
            )

        resultDf
    }

    // Note : Le main attend un argument : le chemin d'accès aux fichiers parquet
    // J'ai compilé et lancé le main depuis le fichier Run_Spark.sh situé à la racine du projet
    def main(args: Array[String]): Unit = {
        
        if (args.length != 1) {
            println("Usage: Gold <inputPath>")
            sys.exit(1)
        }

        val inputPath = args(0)
        val spark = SparkSessionBuilder.SparkSessionBuilder()

        // Génération de la table des établissements par type et région
        val etablissementsParTypeRegionDf = countEtablissementsParTypeRegion(inputPath, spark)
        
        val resultMessage = saveToPostgres(etablissementsParTypeRegionDf, "etablissements_par_type_region")
        println(resultMessage)

        // Génération de la table des pourcentages de réussite au bac général
        val pourcentageReussiteBacGeneralDf = pourcentageReussiteBacGeneral(inputPath, spark)
        val resultMessageBac = saveToPostgres(pourcentageReussiteBacGeneralDf, "pourcentage_reussite_bac_general")
        println(resultMessageBac)
    }
}       