package com.phdata;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.nio.file.*;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

class SparkChargePointsTest {

    private static SparkSession spark;

    @BeforeAll
    static void init() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("spark-test")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void stop() { spark.stop(); }

    private static Object invoke(String name, Class<?>[] sig, Object... args) throws Exception {
        Method m = SparkChargePoints.class.getDeclaredMethod(name, sig);
        m.setAccessible(true);
        return m.invoke(null, args);
    }

    private static Dataset<Row> df(Row... rows) {
        StructType sch = new StructType()
                .add("CPID", "string").add("StartDate", "string").add("StartTime", "string")
                .add("EndDate", "string").add("EndTime", "string");
        return spark.createDataFrame(Arrays.asList(rows), sch);
    }

    private static double[] avgMax(Dataset<Row> res, String id){
        Row r = res.filter(col("chargepoint_id").equalTo(id)).first();
        return new double[]{ r.getDouble(1), r.getDouble(2) };
    }


    @Test  // 1 h + 2 h ⇒ avg 1,5 max 2
    void testAvgDuration() throws Exception {
        Dataset<Row> res = (Dataset<Row>) invoke("transform",
                new Class[]{Dataset.class},
                df(RowFactory.create("A","2025-01-01","00:00","2025-01-01","01:00"),
                   RowFactory.create("A","2025-01-02","00:00","2025-01-02","02:00")));
        double[] v = avgMax(res,"A");
        assertEquals(1.50, v[0], 1e-2);
        assertEquals(2.00, v[1], 1e-2);
    }

    @Test  // cruza meia-noite ⇒ 1,5 h
    void testMaxDurationCrossMidnight() throws Exception {
        Dataset<Row> res = (Dataset<Row>) invoke("transform",
                new Class[]{Dataset.class},
                df(RowFactory.create("B","2025-03-10","23:30","2025-03-11","01:00")));
        double[] v = avgMax(res,"B");
        assertEquals(1.50, v[0], 1e-2);
        assertEquals(1.50, v[1], 1e-2);
    }

    @Test  // vários dias: 2 dias + 30 min = 48,5 h
    void testDurationMultipleDays() throws Exception {
        Dataset<Row> res = (Dataset<Row>) invoke("transform",
                new Class[]{Dataset.class},
                df(RowFactory.create("C","2025-02-01","12:00","2025-02-03","12:30")));
        double[] v = avgMax(res,"C");
        assertEquals(48.50, v[0], 1e-2);
        assertEquals(48.50, v[1], 1e-2);
    }

    @Test  // 1 min ⇒ 0,02 h
    void testMinuteGranularity() throws Exception {
        Dataset<Row> res = (Dataset<Row>) invoke("transform",
                new Class[]{Dataset.class},
                df(RowFactory.create("D","2025-04-01","10:00","2025-04-01","10:01")));
        double[] v = avgMax(res,"D");
        assertEquals(0.02, v[0], 1e-2);
    }

    @Test  // CPIDs independentes
    void testIsolationBetweenChargepoints() throws Exception {
        Dataset<Row> res = (Dataset<Row>) invoke("transform",
                new Class[]{Dataset.class},
                df(RowFactory.create("X","2025-01-01","00:00","2025-01-01","01:00"),
                   RowFactory.create("Y","2025-01-01","00:00","2025-01-01","03:00")));
        double[] x = avgMax(res,"X"); double[] y = avgMax(res,"Y");
        assertAll(
            () -> assertEquals(1.00, x[0], 1e-2),
            () -> assertEquals(3.00, y[1], 1e-2));
    }


    @Test
    void testExtractReadsCsv() throws Exception {
        String csv="CPID,StartDate,StartTime,EndDate,EndTime\nZ,2025-01-01,00:00,2025-01-01,00:05";
        Path p=Files.writeString(Files.createTempFile("cp",".csv"),csv);
        SparkChargePoints.input=p.toString();
        Dataset<Row> df=(Dataset<Row>) invoke("extract",new Class[]{});
        assertEquals(1, df.count());
    }


    @Test
    void testLoadCreatesSingleParquet() throws Exception {
        Dataset<Row> tiny = spark.range(1,4).withColumnRenamed("id","v");
        Path dir=Files.createTempDirectory("pq");
        SparkChargePoints.output=dir.toString();
        invoke("load",new Class[]{Dataset.class}, tiny);
        long files=Files.walk(dir).filter(f->f.toString().endsWith(".parquet")).count();
        assertEquals(1, files);
    }
}
