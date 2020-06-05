package cn.pandadb.bamboo.server

import java.nio.file.Paths

import cn.pandadb.bamboo.server.config.globalConfig
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser
import org.apache.lucene.search.{IndexSearcher, Query}
import org.apache.lucene.store.FSDirectory

import scala.collection.mutable

class VNode(val id: String, var flushInterval: Int = -1) {
  private val dir = FSDirectory.open(Paths.get(s"data/vNode_$id"))
  private val analyzer = new StandardAnalyzer()
  private val writerConfig = new IndexWriterConfig(analyzer)
  writerConfig.setRAMBufferSizeMB(1024)
  writerConfig.setMaxBufferedDocs(10240)
  val writer = new IndexWriter(dir, writerConfig)
  if (flushInterval == -1) {
    flushInterval = globalConfig.flushInterval
  }

  private val task = new java.util.TimerTask {def run() = writer.commit()}
  new java.util.Timer().schedule(task, 0, flushInterval)
  task.run()

  val reader = DirectoryReader.open(dir)
  val searcher = new IndexSearcher(reader)

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

  def write(kv: Map[String, String]): Unit = {
    writer.addDocument(createDocument(kv))
//    writer.commit()
  }

  def search(kv: Map[String, String]): List[Map[String, String]] = {
    val docs = mutable.ListBuffer[Map[String, String]]()
    val hits = searcher.search(buildQuery(kv), 10000) //TODO allow all results
    hits.scoreDocs.map(d => {
      val doc = new mutable.HashMap[String, String]()
      val itl = searcher.doc(d.doc).getFields().iterator()
      while (itl.hasNext()) {
        val field = itl.next()
        doc.put(field.name(), field.stringValue())
      }
      docs += doc.toMap
    })
    docs.toList
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