val scala3Version = "3.5.2"

val sparkVersion = "3.5.3"

fork := true

javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

javaHome := Some(file("C:\\Users\\F6780837\\.vscode\\extensions\\redhat.java-1.36.0-win32-x64\\jre\\17.0.13-win32-x86_64"))


lazy val root = project
  .in(file("."))
  .settings(
    name := "cartao-spark",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      // ("org.scalameta"     %% "munit" % "1.0.2" % Test).cross(CrossVersion.for3Use2_13),
      "org.scalameta"     %% "munit" % "1.0.2" % Test,
      // "org.scalatest"     %% "scalatest" % "3.2.19" % Test,
      ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13),
      ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13),
      ("org.apache.spark" %% "spark-mllib" % sparkVersion).cross(CrossVersion.for3Use2_13)
    )

  )
