name := "collbench"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.scala-miniboxing.plugins" %% "miniboxing-runtime" % "0.4-M8",
  "com.google.guava"  % "guava" % "20.0",
  "net.openhft"       % "zero-allocation-hashing" % "0.6",
  "it.unimi.dsi"      % "fastutil" % "7.0.12",
  "net.sf.trove4j"    % "trove4j" % "3.0.3",
  "com.koloboke"      % "koloboke-api-jdk8" % "1.0.0",
  "com.koloboke"      % "koloboke-impl-jdk8" % "1.0.0",
  "com.goldmansachs"  % "gs-collections" % "7.0.3",
  "com.goldmansachs"  % "gs-collections-api" % "7.0.3",
  "com.carrotsearch"  % "hppc" % "0.7.2",
  "org.scalatest"     %% "scalatest" % "3.0.0" % "test"
)

addCompilerPlugin("org.scala-miniboxing.plugins" %% "miniboxing-plugin" % "0.4-M8")