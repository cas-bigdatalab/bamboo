package cn.pandadb.costore.node.test

import cn.pandadb.costore.node.Index
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.junit.{After, Before, Test}
class QueryCase{

  var writer: IndexWriter = Index.createWriter("./data")

  def createDocument(id: Int, firstName: String, lastName: String, website: String): Document =
  {
    var document: Document = new Document();
    document.add(new StringField("id", id.toString(), Field.Store.YES));
    document.add(new TextField("firstName", firstName, Field.Store.YES));
    document.add(new TextField("lastName", lastName, Field.Store.YES));
    document.add(new TextField("website", website, Field.Store.YES));
    document;
  }

  @Before
  def buildIndex(): Unit ={
    writer.addDocument(createDocument(1, "blue", "joe", "talent.com"))
    writer.addDocument(createDocument(2, "excelwang", "wang", "test.com"))
    writer.addDocument(createDocument(3, "zhongxin", "liu", "angle.com"))
  }

  @After
  def close(): Unit ={
    writer.deleteAll();
    writer.close();
  }

  @Test
  def count(): Unit ={
    println(writer.numRamDocs())
  }
}