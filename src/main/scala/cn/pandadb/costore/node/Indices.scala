package cn.pandadb.costore.node

import java.nio.file.Paths
import java.util

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, StringField, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, IndexableField}
import org.apache.lucene.queryparser.classic.{MultiFieldQueryParser, QueryParser}
import org.apache.lucene.search.{IndexSearcher, Query, ScoreDoc}
import org.apache.lucene.store.FSDirectory

class Indices(dataPath: String) {

  private val dir = FSDirectory.open(Paths.get(dataPath))
  private val analyzer = new StandardAnalyzer()
  private val writerConfig = new IndexWriterConfig(analyzer)
  val writer = new IndexWriter(dir, writerConfig)
  writer.commit()

  val reader = DirectoryReader.open(dir)
  var searcher = new IndexSearcher(reader)

  private def createDocument(kv: Map[String, String]): Document = {
    val document = new Document()
    kv.foreach(
      f => document.add(new TextField(f._1, f._2, Field.Store.YES))
    )
    document
  }

  private def buildQuery(kv: Map[String, String]): Query = {
    new MultiFieldQueryParser(kv.keys.toArray, analyzer).
      parse(kv.values.toArray.mkString(" and "))
  }

  def write(kv: Map[String, String]): Unit ={
    writer.addDocument(createDocument(kv))
    writer.commit()
    println("write: "+kv)
  }

  def search(kv: Map[String, String]): util.ArrayList[util.HashMap[String, String]] = {
    val docs = new util.ArrayList[util.HashMap[String, String]]()
    val hits = searcher.search(buildQuery(kv),10000)//TODO allow all results
    println("query: "+buildQuery(kv))
    println("msg: "+kv)
    println("hit: "+hits.scoreDocs.length)
    hits.scoreDocs.map(d => {
      val doc = new util.HashMap[String, String]()
      val fields = searcher.doc(d.doc).getFields()
      import scala.collection.JavaConversions._
      for (field <- fields){
        doc.put(field.name(), field.stringValue())
      }
      docs.add(doc)
    })
    docs
  }

  def delete(kv: Map[String, String]): Unit = {
    writer.deleteDocuments(buildQuery(kv))
    writer.commit()
  }

  def deleteAll(): Unit = {
    writer.deleteAll()
    writer.commit()
  }
  
}