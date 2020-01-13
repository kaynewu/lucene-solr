/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.kayne;

import java.io.File;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/*******
 *
 * Description: lucene-solr
 * Author: Kaynewu
 * Date: 2020/1/13
 **/
public class TestIndexSorting
{

  private static Directory newDirectory(String indexDir) throws Exception {
    Directory dir = FSDirectory.open(new File(indexDir).toPath());
    return dir;
  }


  private static FieldType TYPE_INDEXED_AND_STORED = new FieldType();

  static {
    TYPE_INDEXED_AND_STORED.setOmitNorms(true);
    TYPE_INDEXED_AND_STORED.setIndexOptions(IndexOptions.DOCS);
    TYPE_INDEXED_AND_STORED.setStored(true);
    TYPE_INDEXED_AND_STORED.setTokenized(false);
    TYPE_INDEXED_AND_STORED.freeze();

  }

  public static void readIndex(String indexDir, Query query)  throws Exception {
    Directory dir = newDirectory(indexDir);
    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(r);
    TopDocs topN = searcher.search(query,100);

    int index = 0;
    int docIdStart = 0;
    LeafReader leafReader = r.leaves().get(index).reader();
    NumericDocValues docValue = DocValues.getNumeric(leafReader, "dense_int");
    for (ScoreDoc sd : topN.scoreDocs) {
      int docID = sd.doc - docIdStart;
      if(docID >= leafReader.maxDoc()){
        index ++;
        docIdStart += leafReader.maxDoc();
        leafReader = r.leaves().get(index).reader();
        docValue = DocValues.getNumeric(leafReader, "dense_int");
        docID = sd.doc - docIdStart;
      }
      Document doc = leafReader.document(docID);
      long value = docValue.get(docID);
      System.out.println("dense_int=" + value + ";dense_string=" + doc.get("dense_string"));
    }
    System.out.println("========================");
    r.close();
  }

  public static void updateIndex(String indexDir ,Term term, Document docment) throws Exception {
    Directory dir = newDirectory(indexDir);
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    SortField sortField = new SortField("dense_int", SortField.Type.INT, true);
    Sort indexSort = new Sort(sortField);
    iwc.setIndexSort(indexSort);
    iwc.setUseCompoundFile(false);
    IndexWriter w = new IndexWriter(dir, iwc);

    w.updateDocument(term, docment);
    w.commit();
    w.flush();
    w.close();
  }


  public static String testIndex(String indexDir ) throws Exception {
    Directory dir = newDirectory(indexDir);
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    SortField sortField = new SortField("dense_int", SortField.Type.INT, true);
    Sort indexSort = new Sort(sortField);
    iwc.setIndexSort(indexSort);
    iwc.setUseCompoundFile(false);
    IndexWriter w = new IndexWriter(dir, iwc);

    for (int i = 0; i < 20; i++) {
      Document doc = new Document();
      long value = Math.abs(TestUtils.random().nextLong() % 10);
      doc.add(new NumericDocValuesField("dense_int", value));
      doc.add(new StoredField("dense_int", value));
      doc.add(new Field("dense_string", value + "", TYPE_INDEXED_AND_STORED));
      w.addDocument(doc);
    }
    w.commit();
    w.close();
    return indexDir;
  }

  public static String optIndex(String indexDir ) throws Exception {
    Directory dir = newDirectory(indexDir);
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    SortField sortField = new SortField("dense_int", SortField.Type.INT, true);
    Sort indexSort = new Sort(sortField);
    iwc.setIndexSort(indexSort);
    iwc.setUseCompoundFile(false);
    IndexWriter w = new IndexWriter(dir, iwc);

    int numDelete = w.numDeletedDocs();
    System.out.println("numDelete:" + numDelete);
    w.forceMerge(1);
    w.close();
    return indexDir;
  }


  public static void main(String[] args) throws Exception {
    String indexDir = "D:\\test\\lucene\\" + System.currentTimeMillis();
    testIndex(indexDir);
    Term term = new Term("dense_string","8");
    Query query = new MatchAllDocsQuery();//new TermQuery(term);
    readIndex(indexDir, query);

    Document docment = new Document();
    long val = 88l;
    docment.add(new NumericDocValuesField("dense_int", val));
    docment.add(new StoredField("dense_int", val));
    docment.add(new Field("dense_string", val + "", TYPE_INDEXED_AND_STORED));

    updateIndex(indexDir, new Term("dense_string","8"), docment);

    updateIndex(indexDir, new Term("dense_string","7"), docment);


    optIndex(indexDir);
    term = new Term("dense_string","88");
    query = new MatchAllDocsQuery(); //new TermQuery(term);
    readIndex(indexDir, query);

    System.exit(0);
  }

}
