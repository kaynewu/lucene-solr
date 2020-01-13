package org.apache.solr.store.hdfs;


import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;



public class HdfsDirectoryIndexTests {

  public static Directory newFsDirectory(String indexDir) throws Exception{
    Directory dir =  new HdfsDirectory(new Path(indexDir), new Configuration());
    return dir;
  }

  private  static Random random = new Random();
  private static FieldType TYPE_INDEXED_AND_STORED = new FieldType();
  static{
    TYPE_INDEXED_AND_STORED.setOmitNorms(true);
    TYPE_INDEXED_AND_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_INDEXED_AND_STORED.setStored(true);
    TYPE_INDEXED_AND_STORED.setTokenized(false);
    TYPE_INDEXED_AND_STORED.freeze();
  }

  public static void writerIndex(String dirPath) throws Exception {
    Directory dir = newFsDirectory(dirPath);
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    IndexWriter w = new IndexWriter(dir, iwc);
    for (int i = 0; i < 20000000; i++) {
      Document doc = new Document();
      long value = random.nextLong();
      doc.add(new NumericDocValuesField("id_value", value));
      doc.add(new LongPoint("id", value));
      w.addDocument(doc);
    }
    w.commit();
    w.forceMerge(1);
    IOUtils.close(w, dir);

  }



  public static void readIndex(String dirPath) throws Exception {
    Directory dir = newFsDirectory(dirPath);
    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    Query query =  new MatchAllDocsQuery();
    TopDocs topN = searcher.search(query, 10);

    List<LeafReaderContext> r = reader.getContext().leaves();

    NumericDocValues numberDocValues = r.get(0).reader().getNumericDocValues("id_value");
    for (ScoreDoc doc : topN.scoreDocs) {
      int docId = doc.doc;
      numberDocValues.advance(docId);
      System.out.println(docId + ":" + numberDocValues.longValue());
    }
  }

  public static void main(String[] args) throws Exception {
    System.setProperty("hadoop.home.dir","D:\\local-soft\\winutils-master\\hadoop-3.2.0");
    for(int i = 0; i < 10; i ++)
      writerIndex("file:///D:/test/pointIndex/dir" + i);
  }
}
