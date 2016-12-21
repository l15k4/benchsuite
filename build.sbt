name := "collbench"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.rklaehn"       %% "abc"                    % "0.3.0",
  "com.google.guava"  % "guava"                   % "20.0",
  "net.openhft"       % "zero-allocation-hashing" % "0.6",
  "com.storm-enroute" %% "scalameter"             % "0.8.2" % "test"
)

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")

logBuffered := false

parallelExecution in Test := false

enablePlugins(JmhPlugin)
sourceDirectory in Jmh := (sourceDirectory in Test).value
classDirectory in Jmh := (classDirectory in Test).value
dependencyClasspath in Jmh := (dependencyClasspath in Test).value
compile in Jmh <<= (compile in Jmh) dependsOn (compile in Test)
run in Jmh <<= (run in Jmh) dependsOn (Keys.compile in Jmh)