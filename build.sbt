organization := "org.allenai"

name := "wikitables-executor"

description := "Simple code to execute WikiTableQuestions logical forms using SEMPRE"

version := "0.1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "17.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0"
)

mainClass in assembly := Some("org.allenai.wikitables.Executor")

fork := true

cancelable in Global := true
