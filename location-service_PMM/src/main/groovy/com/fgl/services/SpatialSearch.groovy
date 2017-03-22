package com.fgl.services

import com.spatial4j.core.context.SpatialContext
import com.spatial4j.core.distance.DistanceUtils
import com.spatial4j.core.exception.InvalidShapeException
import com.spatial4j.core.shape.Point
import com.spatial4j.core.shape.Shape
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.document.Field.Store
import org.apache.lucene.index.*
import org.apache.lucene.queries.function.ValueSource
import org.apache.lucene.search.*
import org.apache.lucene.spatial.SpatialStrategy
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree
import org.apache.lucene.spatial.query.SpatialArgs
import org.apache.lucene.spatial.query.SpatialOperation
import org.apache.lucene.store.Directory
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.util.Version

public class SpatialSearch {

    private IndexWriter indexWriter;
    private IndexReader indexReader;
    private IndexSearcher searcher;
    private SpatialContext ctx;
    private SpatialStrategy strategy;

    public SpatialSearch(String indexPath) {

        StandardAnalyzer a = new StandardAnalyzer(Version.LATEST);
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LATEST, a);
        Directory directory;

        try {
            directory = new SimpleFSDirectory(new File(indexPath));
            indexWriter = new IndexWriter(directory, iwc);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        this.ctx = SpatialContext.GEO;

        SpatialPrefixTree grid = new GeohashPrefixTree(ctx, 11);
        this.strategy = new RecursivePrefixTreeStrategy(grid, "location");
    }


    public void indexStoreDocuments(List<com.fgl.domain.Store>stores) throws IOException {

        stores.eachWithIndex{store,index->


            // some of them don't have long and lat
            if (store.latitude && store.longitude){

                try {
                    indexWriter.addDocument(newGeoDocument(index, store.storeNumber, ctx.makePoint((store.longitude), (store.latitude))));

                }
                catch(InvalidShapeException e){
                    println "bad long/lat data for store ${store.storeNumber}"

                }


            }

       }


        indexWriter.commit();
        indexWriter.close();
    }


    private Document newGeoDocument(int id, String name, Shape shape) {

        FieldType ft = new FieldType();
        ft.setIndexed(true);
        ft.setStored(true);

        Document doc = new Document();

        doc.add(new IntField("id", id, Store.YES));
        doc.add(new Field("storeNumber", name, ft));
        for(IndexableField f:strategy.createIndexableFields(shape)) {
            doc.add(f);
        }

        doc.add(new StoredField(strategy.getFieldName(), ctx.toString(shape)));

        return doc;
    }

    public void setSearchIndexPath(String indexPath) throws IOException{

        File file = new File(indexPath)

        this.indexReader = DirectoryReader.open(new SimpleFSDirectory(file));
        this.searcher = new IndexSearcher(indexReader);
    }

    public Map<String,String> search(Double lng, Double lat, int distance) throws IOException{

        // create empty map
        def stores = [:]


        Point p = ctx.makePoint(lng, lat);
        SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects,ctx.makeCircle(lng, lat, DistanceUtils.dist2Degrees(distance, DistanceUtils.EARTH_MEAN_RADIUS_KM)));
        Filter filter = strategy.makeFilter(args);

        ValueSource valueSource = strategy.makeDistanceValueSource(p);
        Sort distSort = new Sort(valueSource.getSortField(false)).rewrite(searcher);

        int limit = 10;
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), filter, limit, distSort);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;



        for(ScoreDoc s: scoreDocs) {

            Document doc = searcher.doc(s.doc);
            Point docPoint = (Point) ctx.readShape(doc.get(strategy.getFieldName()));
            double docDistDEG = ctx.getDistCalc().distance(args.getShape().getCenter(), docPoint);
            double docDistInKM = DistanceUtils.degrees2Dist(docDistDEG, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM);

            // return a list of store ids and we can map that to stores later

            def storeNumber = doc.get("storeNumber")

            // add to map
            stores.put(storeNumber,docDistInKM)

            println("Results: ${doc.get("id")}  ${doc.get("storeNumber")} + ${docDistInKM}km ");




        }


        stores

    }


    public setUpIndex(indexPath,stores){

        //Indexes sample documents
        indexStoreDocuments(stores);
        setSearchIndexPath(indexPath);

    }


}
