import AssemblyKeys._ 

assemblySettings


name := "gbs-demultiplexer"

version := "0.4"

scalaVersion := "2.10.0"

libraryDependencies += "org.rogach" %% "scallop" % "0.8.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
 
libraryDependencies +=
  "com.typesafe.akka" %% "akka-actor" % "2.1.0"
