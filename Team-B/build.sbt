name := "Big_Data_P3"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "2.3.2",
                        "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2",
                        "org.apache.kafka" % "kafka-clients" % "2.6.0")
