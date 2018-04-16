package org.allenai.wikitables

import java.io.PrintWriter

import scala.io.Source
import scala.collection.JavaConverters._

import edu.stanford.nlp.sempre._
import edu.stanford.nlp.sempre.tables._
import edu.stanford.nlp.sempre.tables.test.CustomExample
import edu.stanford.nlp.sempre.tables.dpd.DPDParser
import edu.stanford.nlp.sempre.tables.TableKnowledgeGraph
import edu.stanford.nlp.sempre.tables.lambdadcs.LambdaDCSExecutor
import fig.basic.LispTree

object Executor {
  def main(args: Array[String]): Unit = {
    if (args(0) == "serve") {
      serve(args(1))
    } else {
      evaluateFile(args)
    }
  }

  def evaluateFile(args: Array[String]): Unit = {
    val exampleFile = args(0)
    val logicalFormFile = args(1)
    val outputDenotationFile = logicalFormFile.replace(".txt", "_denotations.tsv")
    val tableDirectory = args(2)
    TableKnowledgeGraph.opts.baseCSVDir = tableDirectory
    val targetPreprocessor = new TableValuePreprocessor()

    val builder = getSempreBuilder()
    val executor = builder.executor
    val valueEvaluator = builder.valueEvaluator
    val examples = for (line <- Source.fromFile(exampleFile).getLines) yield line
    val logicalForms = for (line <- Source.fromFile(logicalFormFile).getLines) yield line
    val denotations = (examples.zip(logicalForms).toList.par.map { case (exampleString, logicalForm) => {
      if (logicalForm.startsWith("Error producing")) {
        ("", 0.0)
      } else if (logicalForm.equals("")) {
        ("", 0.0)
      } else{
        try {
          val sempreFormula = Formula.fromString(logicalForm)
          val lispTree = LispTree.proto.parseFromString(exampleString)
          val example = CustomExample.fromLispTree(lispTree, "")
          val predicted = executor.execute(sempreFormula, example.context).value
          val target = targetPreprocessor.preprocess(example.targetValue)
          val result = valueEvaluator.getCompatibility(target, predicted)
          (predicted.toString, result)
        } catch {
          case e: Exception => {
            println("Error parsing logical form: " + logicalForm)
            ("", 0.0)
          }
        }
      }
    }}).seq.toList

    val writer = new PrintWriter(outputDenotationFile)
    denotations.foreach { case (denotation, correct) => {
      writer.write(correct + "\t" + denotation + "\n")
    }}
    writer.close()

    println("Total examples evaluated: " + denotations.size)
    println("Average accuracy: " + (denotations.map(_._2).sum / denotations.size))
  }

  def serve(tableDirectory: String) {
    TableKnowledgeGraph.opts.baseCSVDir = tableDirectory
    val targetPreprocessor = new TableValuePreprocessor()

    val builder = getSempreBuilder()
    val executor = builder.executor
    val valueEvaluator = builder.valueEvaluator
    while (true) {
      val exampleString = scala.io.StdIn.readLine()
      val logicalForm = scala.io.StdIn.readLine()
      try {
        val sempreFormula = Formula.fromString(logicalForm)
        val lispTree = LispTree.proto.parseFromString(exampleString)
        val example = CustomExample.fromLispTree(lispTree, "")
        val predicted = executor.execute(sempreFormula, example.context).value
        val target = targetPreprocessor.preprocess(example.targetValue)
        val result = valueEvaluator.getCompatibility(target, predicted)
        println(result)
      } catch {
        case e: Exception => {
          println(0.0)
        }
      }
    }
  }

  def getSempreBuilder(): Builder = {
    // Setting all the options typically selected by Sempre
    // TODO: Make these actual command line arguments.
    Builder.opts.parser = "tables.dpd.DPDParser"
    CustomExample.opts.allowNoAnnotation = true
    DPDParser.opts.cheat = true
    DPDParser.opts.dpdParserBeamSize = 100
    Builder.opts.executor = "tables.lambdadcs.LambdaDCSExecutor"
    Builder.opts.valueEvaluator = "tables.TableValueEvaluator"
    LambdaDCSExecutor.opts.genericDateValue = true
    TableValueEvaluator.opts.allowMismatchedTypes = true
    TargetValuePreprocessor.opts.targetValuePreprocessor = "tables.TableValuePreprocessor"
    StringNormalizationUtils.opts.numberCanStartAnywhere = true
    StringNormalizationUtils.opts.num2CanStartAnywhere = true
    NumberFn.opts.unitless = true
    NumberFn.opts.alsoTestByConversion = true
    NumberFn.opts.alsoTestByIsolatedNER = true
    JoinFn.opts.specializedTypeCheck = false
    JoinFn.opts.typeInference = true
    TypeInference.opts.typeLookup = "tables.TableTypeLookup"
    FloatingParser.opts.useSizeInsteadOfDepth = true
    FloatingParser.opts.maxDepth = 8
    FloatingParser.opts.useAnchorsOnce = false
    FloatingParser.opts.useMaxAnchors = 2
    DerivationPruner.opts.pruningStrategies = List("emptyDenotation", "nonLambdaError", "atomic",
      "tooManyValues", "badSummarizerHead", "mistypedMerge", "doubleNext", "doubleSummarizers",
      "sameMerge", "unsortedMerge", "typeRowMerge").asJava
    DerivationPruner.opts.pruningComputers = List("tables.TableDerivationPruningComputer").asJava
    DerivationPruner.opts.recursivePruning = false
    Grammar.opts.inPaths = List("data/grow.grammar").asJava
    Grammar.opts.binarizeRules = false
    Grammar.opts.tags = List("scoped", "merge-and", "arithmetic", "comparison", "alternative",
      "neq", "yearrange", "part", "closedclass", "scoped-2args-merge-and").asJava
    // End of command line arguments.
    val builder = new Builder()
    builder.build()
    builder
  }
}
