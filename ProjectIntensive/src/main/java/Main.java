import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getRootLogger().setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName(Main.class.getName())
                .setMaster("local[*]")
                .set("spark.driver.host", "localhost");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName("2ID70")
                .getOrCreate();

        Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds = Q1(sc, spark);
        Q2(rdds,spark);
        Q3(rdds);
        Q4(rdds,spark);
    }

    public static Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> Q1(JavaSparkContext sc, SparkSession spark) {
        //path
        String patientsPath = "C:/Users/perem/Desktop/3AI/DataIntensive/Assignment 2/assignment_datasets/patients.csv";
        String prescriptionsPath = "C:/Users/perem/Desktop/3AI/DataIntensive/Assignment 2/assignment_datasets/prescriptions.csv";
        String diagnosesPath = "C:/Users/perem/Desktop/3AI/DataIntensive/Assignment 2/assignment_datasets/diagnoses.csv";

        // Regex for date validation (yyyy-mm-dd)
        Pattern datePattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

        try {
            JavaRDD<String> rawPatients = sc.textFile(patientsPath);

            JavaRDD<String> filteredPatients = rawPatients.filter(line -> {
                String[] parts = line.split(",");

                // Must have exactly 4 attributes
                if (parts.length != 4) {
                    return false;
                }

                // Check if patientId is a valid integer
                if (!parts[0].matches("\\d+")) {
                    return false;
                }

                // Validate date format (yyyy-mm-dd)
                if (!datePattern.matcher(parts[3].trim()).matches()) {
                    return false;
                }

                return true;
            });

            // DELETE LATER, PRINT ONLY FOR SHOWCASE
            JavaRDD<String> deletedRecords = rawPatients.subtract(filteredPatients);

            // DELETE LATER, PRINT ONLY FOR SHOWCASE
            long totalBefore = rawPatients.count();
            long totalAfter = filteredPatients.count();
            long totalDeleted = totalBefore - totalAfter;

            // DELETE LATER, PRINT ONLY FOR SHOWCASE
            System.out.println("\nTotal records before filtering: " + totalBefore);
            System.out.println("Total records after filtering: " + totalAfter);
            System.out.println("Number of records removed (corrupt records): " + totalDeleted);

            JavaRDD<Row> patientsRowRDD = filteredPatients.map(line -> {
                String[] parts = line.split(",");
                return RowFactory.create(
                        Integer.parseInt(parts[0]),  // patientId
                        parts[1],                    // patientName
                        parts[2],                    // address
                        parts[3]                     // dateOfBirth
                );
            });

            StructType patientsSchema = new StructType(new StructField[]{
                    new StructField("patientId", DataTypes.IntegerType, false, Metadata.empty()),
                    new StructField("patientName", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("address", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("dateOfBirth", DataTypes.StringType, false, Metadata.empty())
            });

            Dataset<Row> patientsDF = spark.createDataFrame(patientsRowRDD, patientsSchema);

            patientsDF = patientsDF
                    .withColumn("year", patientsDF.col("dateOfBirth").substr(1, 4))
                    .withColumn("month", patientsDF.col("dateOfBirth").substr(6, 2))
                    .withColumn("day", patientsDF.col("dateOfBirth").substr(9, 2));

            // DELETE LATER, PRINT ONLY FOR SHOWCASE
            long numPatientsDF = patientsDF.count();
            System.out.println("\nFinal number of valid patients in DataFrame: " + numPatientsDF);

            patientsDF.createOrReplaceTempView("patients");

            // DELETE LATER, PRINT ONLY FOR SHOWCASE
            System.out.println("\nSample rows AFTER filtering:");
            List<String> filteredSampleRows = filteredPatients.take(5);
            for (String row : filteredSampleRows) {
                System.out.println(row);
            }

            // DELETE LATER, PRINT ONLY FOR SHOWCASE
            System.out.println("\nSample of 10 deleted (corrupt) records:");
            List<String> deletedSampleRows = deletedRecords.take(10);
            for (String row : deletedSampleRows) {
                System.out.println(row);
            }

            return new Tuple3<>(filteredPatients, sc.textFile(prescriptionsPath), sc.textFile(diagnosesPath));
        } catch (Exception e) {
            System.err.println("Error loading files: " + e.getMessage());
            e.printStackTrace();
            return new Tuple3<>(null, null, null);
        }
    }

    public static void Q2(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds, SparkSession spark) {
        //Create a table for prescriptions
        JavaRDD<String> prescriptionRDD = rdds._2();

        StructType prescriptionSchema = new StructType(new StructField[]{
                new StructField("prescriptionId", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("medicineId", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("dossage", DataTypes.StringType, false, Metadata.empty()),

        });

        JavaRDD<Row> prescriptionRowRDD = prescriptionRDD.map(line -> {
            String[] parts = line.split(",");
            return RowFactory.create(
                    Integer.parseInt(parts[0]),  // prescriptionId
                    Integer.parseInt(parts[1]),  // medicineId
                    parts[2]                     // dossage

            );
        });

        Dataset<Row> prescriptionDF = spark.createDataFrame(prescriptionRowRDD, prescriptionSchema);
        prescriptionDF.createOrReplaceTempView("prescription");

        //Create a table for diagnoses
        JavaRDD<String> diagnosesRDD = rdds._3();

        StructType diagnosesSchema = new StructType(new StructField[]{
                new StructField("patientId", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("doctorId", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("diagnosisDate", DataTypes.StringType, false, Metadata.empty()),
                new StructField("diagnosis", DataTypes.StringType, false, Metadata.empty()),
                new StructField("prescriptionId", DataTypes.IntegerType, false, Metadata.empty())

        });

        JavaRDD<Row> diagnosesRowRDD = diagnosesRDD.map(line -> {
            String[] parts = line.split(",");
            return RowFactory.create(
                    Integer.parseInt(parts[0]),  // patientId
                    Integer.parseInt(parts[1]),  // doctorId
                    parts[2],                    // diagnosisDate
                    parts[3],                    // diagnosis
                    Integer.parseInt(parts[4])   // prescriptionId
            );
        });

        Dataset<Row> diagnosesDF = spark.createDataFrame(diagnosesRowRDD, diagnosesSchema);
        diagnosesDF = diagnosesDF
                .withColumn("year", diagnosesDF.col("diagnosisDate").substr(1, 4))
                .withColumn("month", diagnosesDF.col("diagnosisDate").substr(6, 2))
                .withColumn("day", diagnosesDF.col("diagnosisDate").substr(9, 2));
        diagnosesDF.createOrReplaceTempView("diagnoses");

        //q2 starts
        Dataset<Row> result = spark.sql("SELECT COUNT(*) FROM patients WHERE year = '1999'");

        long q21 = result.collectAsList().get(0).getLong(0);

        System.out.println(">> [q21: " + q21 + "]");

        Dataset<Row> date = spark.sql(
                "SELECT diagnosisDate, COUNT(*) AS count " +
                        "FROM diagnoses " +
                        "WHERE diagnosisDate LIKE '2024%' " +
                        "GROUP BY diagnosisDate " +
                        "ORDER BY count DESC " +
                        "LIMIT 1"
        );

        String q22 = date.first().getString(0);
        System.out.println(">> [q22: " + q22 + "]");

        //"SELECT prescriptionId, date FROM prescription, JOIN diagnose"
        Dataset<Row> max_prescription_date = spark.sql(
                "SELECT d.diagnosisDate " +
                        "FROM diagnoses d " +
                        "JOIN ( " +
                        "    SELECT prescriptionId, COUNT(*) as medicineCount " + // COUNT(*) now sufficient
                        "    FROM prescription " +
                        "    GROUP BY prescriptionId " +
                        "    ORDER BY medicineCount DESC " +
                        "    LIMIT 1 " +
                        ") as maxPrescription ON d.prescriptionId = maxPrescription.prescriptionId " +
                        "WHERE SUBSTR(d.diagnosisDate, 1, 4) = '2024'"
        );

        String q23 = max_prescription_date.collectAsList().get(0).getString(0);
        System.out.println(">> [q23: " + q23 + "]");
    }

    public static void Q3(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds) {
        JavaRDD<String> patientsBornIn1999 = rdds._1().filter(line -> {
            String[] parts = line.split(",");
            return parts[3].startsWith("1999");
        });

        long totalCount= patientsBornIn1999.count();

        var q31 = totalCount;

        System.out.println(">> [q31: " + q31 + "]");

        JavaRDD<String> diagnosesRDD = rdds._3();

        JavaRDD<String> diagnosisDates = diagnosesRDD
                .map(line -> {
                    String[] parts = line.split(",");
                    if (parts.length >= 3) {
                        return parts[2].trim();
                    }
                    return null;
                })
                .filter(date -> date != null && !date.isEmpty());

        JavaRDD<String> diagnosisDates2024 = diagnosisDates
                .filter(date -> date.startsWith("2024-"));

        JavaPairRDD<String, Integer> dateCounts = diagnosisDates2024
                .mapToPair(date -> new Tuple2<>(date, 1))
                .reduceByKey(Integer::sum);

        Tuple2<String, Integer> maxDate = dateCounts
                .reduce((a, b) -> a._2 > b._2 ? a : b);  // Compare counts to find the max

        System.out.println(">> [q32: " + maxDate._1 + "]");


        //Create tuples with <id,1>
        JavaPairRDD<Integer, Integer> id_one = rdds._2().mapToPair(line ->{
            String[] parts = line.split(",");
            return new Tuple2<>(Integer.parseInt(parts[0]), 1);
        }) ;

        //Create tuples with <id,sum> by summing corresponding key's values
        JavaPairRDD<Integer,Integer> id_sum = id_one.reduceByKey(Integer::sum);

        //Lets create our other rdd based on diagnosis in order to recieve date <id,date>
        JavaPairRDD<Integer , String> id_date = rdds._3().mapToPair(line ->{
            String[] parts = line.split(",");
            return new Tuple2<>(Integer.parseInt(parts[4]), parts[2]);
        });
        //Join those <id,date,sum>
        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRDD = id_sum.join(id_date);

        //filter and select with 2024 <id,date,sum>
        JavaPairRDD<Integer, Tuple2<Integer, String>> id_date_2024 = joinedRDD.filter(tuple -> {
            String date = tuple._2()._2(); // Get the date (value of the tuple)
            String year = date.substring(0, 4); // Extract the year
            return year.equals("2024"); // Keep if year is "2024"
        });

        //reduce
        // Find the record with the maximum sum.
        Tuple2<Integer, Tuple2<Integer, String>> maxRecord = id_date_2024.reduce((g1, g2) -> {
            if (g1._2()._1() > g2._2()._1()) {
                return g1;
            } else {
                return g2;
            }
        });

        String final_date = "";

        final_date = maxRecord._2()._2();

        var q33 = final_date;
        System.out.println(">> [q33: " + q33 + "]");
    }

    public static void Q4(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds,SparkSession spark
    ) {
        JavaRDD<String> diagnosesRDD = rdds._3();

        StructType diagnosesSchema = new StructType(new StructField[]{
                new StructField("patientId", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("doctorId", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("diagnosisDate", DataTypes.StringType, false, Metadata.empty()),
                new StructField("diagnosis", DataTypes.StringType, false, Metadata.empty()),
                new StructField("prescriptionId", DataTypes.IntegerType, false, Metadata.empty())

        });

        JavaRDD<Row> diagnosesRowRDD = diagnosesRDD.map(line -> {
            String[] parts = line.split(",");
            return RowFactory.create(
                    Integer.parseInt(parts[0]),  // patientId
                    Integer.parseInt(parts[1]),  // doctorId
                    parts[2],                    // diagnosisDate
                    parts[3],                    // diagnosis
                    Integer.parseInt(parts[4])   // prescriptionId
            );
        });
        Dataset<Row> diagnosesDF = spark.createDataFrame(diagnosesRowRDD, diagnosesSchema);


        Dataset<Row> diagnosesMonth = diagnosesDF
                .withColumn("diagnosisDate", functions.to_date(functions.col("diagnosisDate"), "yyyy-MM-dd"))
                .filter(functions.col("diagnosisDate").isNotNull())
                .withColumn("month", functions.date_format(functions.col("diagnosisDate"), "yyyy-MM"));

        Dataset<Row> diagnosesMonthDoctor = diagnosesMonth;

        Dataset<Row> doctorDiagnosisCount = diagnosesMonthDoctor
                .groupBy("doctorId", "month", "diagnosis")
                .agg(functions.count("diagnosis").alias("diagnosisCount"));


        WindowSpec windowSpec = Window.partitionBy("doctorId", "month")
                .orderBy(functions.desc("diagnosisCount"));

        Dataset<Row> doctorDiagnosisCountSorted = doctorDiagnosisCount
                .withColumn("row_number", functions.row_number().over(windowSpec))
                .where(functions.col("row_number").equalTo(1))
                .select("month", "doctorId", "diagnosis");

        Dataset<Row> mostFrequentDiagnosisCount = doctorDiagnosisCountSorted
                .groupBy("month", "diagnosis")
                .agg(functions.count("diagnosis").as("diagnosisCount"));

        Dataset<Row> totalDoctorPerMonth = diagnosesMonthDoctor
                .groupBy("month")
                .agg(functions.countDistinct("doctorId").as("totalDoctorCount"));

        Dataset<Row> epidemicMonths = mostFrequentDiagnosisCount.join(totalDoctorPerMonth, "month");
        Dataset<Row> pairs = epidemicMonths
                .where(functions.col("diagnosisCount").gt(functions.col("totalDoctorCount").multiply(0.5)))
                .select("month", "diagnosis");


        List<Row> results = pairs.collectAsList();
        StringBuilder output = new StringBuilder(">> [q4:");
        for (Row row : results) {
            String month = row.getAs("month");
            String diagnosis = row.getAs("diagnosis");
            output.append(String.format(" %s,%s;", month, diagnosis));
        }
        output.setLength(output.length() - 1);
        System.out.println(output + "]");
    }
}