import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject

import com.mongodb.{
    MongoClient,
    MongoException,
    WriteConcern,
    DB,
    DBCollection,
    BasicDBObject,
    BasicDBList,
    DBObject,
    DBCursor
}

import com.mongodb.hadoop.{
    MongoInputFormat,
    MongoOutputFormat,
    BSONFileInputFormat,
    BSONFileOutputFormat
}

//import com.mongodb.casbah.Imports._ /////

import com.mongodb.hadoop.io.MongoUpdateWritable

import org.apache.log4j.{Level, Logger} //this is simply used to eliminate unnecesary info from the console

import java.lang.Math

object MongoSpark {
    def main(args: Array[String]) {
        /* Uncomment to turn off Spark logs */
        //Logger.getLogger("org").setLevel(Level.OFF)
        //Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("MongoSpark")
            .set("spark.driver.memory", "2g")
            .set("spark.executor.memory", "4g")

        val sc = new SparkContext(conf)
		
		
		//this is simply used to eliminate unnecesary info from the console
		val rootLogger = Logger
			.getRootLogger()
			.setLevel(Level.ERROR)
			
		
        val article_input_conf = new Configuration()
        article_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dblp.Article")

        val inproceedings_input_conf = new Configuration()
        inproceedings_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dblp.Inproceedings")
		
		
        val article = sc.newAPIHadoopRDD(
            article_input_conf,         // config
            classOf[MongoInputFormat],  // input format
            classOf[Object],            // key type
            classOf[BSONObject]         // val type
        )

        val inproceedings = sc.newAPIHadoopRDD(
            inproceedings_input_conf,
            classOf[MongoInputFormat],
            classOf[Object],
            classOf[BSONObject]
        )

		
        q1(article, inproceedings)
        q2(inproceedings)
        q3a(inproceedings)
        q3b(inproceedings)
        q3c(inproceedings)
        q3d(article, inproceedings)
        q3e(article, inproceedings)
        q4a(article, inproceedings)
        q4b(article, inproceedings)
		
		
    }

    /* Q1. 
     * Count the number of tuples. */
    def q1(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        println("Article size:")
		println(article.count())
		
		println("\nInproceedings size:")
		println(inproceedings.count())
    }

    /* Q2.
     * Add a column "Area" in the Inproceedings table.
     * Then, populate the column "Area" with the values from the above table if
     * there is a match, otherwise set it to "UNKNOWN" */
    def q2(inproceedings: RDD[(Object,BSONObject)]) {
			
		val outputConfig = new Configuration()
		outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/dblp.Inproceedings")
		
		val database = List("SIGMOD Conference", "VLDB", "ICDE", "PODS")
		val theory = List("STOC", "FOCS", "SODA", "ICALP")
		val systems = List("SIGCOMM", "ISCA", "HPCA", "PLDI")
		val ml_ai = List("ICML", "NIPS", "AAAI", "IJCAI")
		val longList = database:::theory:::systems:::ml_ai
		

		val update1 = inproceedings
			.filter( x => !longList.contains(x._2.get("booktitle")) )
			.mapValues(value => new MongoUpdateWritable(
				new BasicDBObject("_id", value.get("_id")),  // Query
		      	new BasicDBObject("$set", new BasicDBObject("area", "UNKNOWN")),  // Update operation
		      	false,  // Upsert
		      	false,	// Update multiple documents
				false))
				
		val update2 = inproceedings
			.filter( x => database.contains(x._2.get("booktitle")) )
			.mapValues(value => new MongoUpdateWritable(
				new BasicDBObject("_id", value.get("_id")),  // Query
		      	new BasicDBObject("$set", new BasicDBObject("area", "Database")),  // Update operation
		      	false,  // Upsert
		      	false,	// Update multiple documents
				false))
				
		val update3 = inproceedings
			.filter( x => theory.contains(x._2.get("booktitle")) )
			.mapValues(value => new MongoUpdateWritable(
				new BasicDBObject("_id", value.get("_id")),  // Query
		      	new BasicDBObject("$set", new BasicDBObject("area", "Theory")),  // Update operation
		      	false,  // Upsert
		      	false,	// Update multiple documents
				false))
				
		val update4 = inproceedings
			.filter( x => systems.contains(x._2.get("booktitle")) )
			.mapValues(value => new MongoUpdateWritable(
				new BasicDBObject("_id", value.get("_id")),  // Query
		      	new BasicDBObject("$set", new BasicDBObject("area", "Systems")),  // Update operation
		      	false,  // Upsert
		      	false,	// Update multiple documents
				false))
				
		val update5 = inproceedings
			.filter( x => ml_ai.contains(x._2.get("booktitle")) )
			.mapValues(value => new MongoUpdateWritable(
				new BasicDBObject("_id", value.get("_id")),  // Query
		      	new BasicDBObject("$set", new BasicDBObject("area", "ML-AI")),  // Update operation
		      	false,  // Upsert
		      	false,	// Update multiple documents
				false))
				
		
		val update = update1.union(update2).union(update3).union(update4).union(update5)

		
		update.saveAsNewAPIHadoopFile(
			"file:///this-is-completely-unused",
		    classOf[Object],
		    classOf[MongoUpdateWritable],
		    classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
		    outputConfig)
		
    }

    /* Q3a.
     * Find the number of papers in each area above. */
    def q3a(inproceedings: RDD[(Object,BSONObject)]) {
		
		val output = inproceedings
			.map{ case(id, item) => (item.get("area"), 1) }
			.reduceByKey( (a, b) => (a + b) )
		
		println("\nQ3a:")

		output.foreach(println)
		
    }

    /* Q3b.
     * Find the TOP­20 authors who published the most number of papers in
     * "Database" (author published in multiple areas will be counted in all those
     * areas). */
    def q3b(inproceedings: RDD[(Object,BSONObject)]) {
		
        val output = inproceedings
			.filter(x => x._2.get("area")=="Database")
			.map{ case(id, item) => item.get("authors").toString().stripPrefix("[ ").stripSuffix("]")}
			.flatMap(x => x.split(" , "))
			.map(x => (x, 1))
			.reduceByKey( (a, b) => (a + b) )	
		
		println("\nQ3b:")
			
		output.takeOrdered(20)(Ordering[Int].reverse.on(x=>x._2)).foreach(println)
    }

    /* Q3c.
     * Find the number of authors who published in exactly two of the four areas
     * (do not consider UNKNOWN). */
    def q3c(inproceedings: RDD[(Object,BSONObject)]) {
        val output = inproceedings
			.filter(x => x._2.get("area")!="UNKNOWN") 			// remove docs with area=UNKNOWN
			.map{ case(id, item) => ( item.get("area"), item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") )}
			.flatMapValues(authors => authors.split(" , "))		// (area, author)
			.distinct()											// exclude repetitions in the area
			.map{ case(area, author) => (author, 1) }
			.reduceByKey( (a, b) => (a + b) )					// count number of distinc areas for each author
			.filter(x => x._2==2)								// authors publishing in exactly 2 areas
						
		println("\nQ3c:")
			
		println(output.count())
    }

    /* Q3d.
     * Find the number of authors who wrote more journal papers than conference
     * papers (irrespective of research areas). */
    def q3d(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        val conf = inproceedings
			.map{ case(id, item) => item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") }		// authors as a string
			.flatMap(x => x.split(" , "))						// author
			.filter( x => x!="")								// remove rows with empty author
			.map(x => (x, 1))		
			.reduceByKey( (a, b) => (a + b) )					// (author, numbOfConfPapers)
			
		val journ = article
			.map{ case(id, item) => item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") }
			.flatMap(x => x.split(" , "))
			.filter( x => x!="")
			.map(x => (x, 1))
			.reduceByKey( (a, b) => (a + b) )					// (author, numbOfJournPapers)
	
			
		val output = journ
			.leftOuterJoin(conf)
			.map { case (a, (b, c: Option[Int])) => (a, b, c.getOrElse(0)) }
			.filter{ case(a, b, c) => b>c }						
		
		println("\nQ3d:")
		
		println(output.count())
    }

    /* Q3e.
     * Find the top­5 authors who published the maximum number of papers (journal
     * OR conference) since 2000, among the authors who have published at least one
     * "Database" paper (in a conference). */
    def q3e(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        
		val db = inproceedings
			.filter(x => x._2.get("area")=="Database")
			.map{ case(id, item) => item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") }
			.flatMap(x => x.split(" , "))
			.distinct()
			.map(x => (x, 1))
			
		val journ = article
			.filter(x => x._2.get("year")!=null)
			.filter(x => x._2.get("year").toString().toInt>=2000)
			.map{ case(id, item) => item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") }
			.flatMap(x => x.split(" , "))
			.filter(author => author!="")
			.map(x => (x, 1))
			.reduceByKey( (a, b) => (a + b) )
		
		val conf = inproceedings
			.filter(x => x._2.get("year").toString().toInt>=2000)
			.map{ case(id, item) => item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") }
			.flatMap(x => x.split(" , "))
			.filter(author => author!="")
			.map(x => (x, 1))
			.reduceByKey( (a, b) => (a + b) )
			
		val total = journ
			.fullOuterJoin(conf)
			.map { case (a, (b: Option[Int], c: Option[Int])) => (a, b.getOrElse(0) + c.getOrElse(0) ) }
		
		
		val output = db
			.leftOuterJoin(total)
			.map { case (a, (b, c: Option[Int])) => (a, c.getOrElse(0)) }
		
		println("\nQ3e:")
		
		output.takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2)).foreach(println)			
		 
    }

    /* Q4a.
     * Plot a linegraph with two lines, one for the number of journal papers and
     * the other for the number of conference paper in every decade starting from
     * 1950.
     * Therefore the decades will be 1950-1959, 1960-1969, ...., 2000-2009,
     * 2010-2015. */
    def q4a(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
       
		val journ = article
			.filter(x => x._2.get("year")!=null)
			.map{ case(id, item) => item.get("year").toString().toInt}
			.filter(x => x>=1950)
			.map(x => Math.floor(x/10)*10 )
			.map(x => (x.toInt, 1) )
			.reduceByKey( (a, b) => (a + b) )
			
		println("\nQ4a: Journal Papers")
			
		journ.takeOrdered(7).foreach(println) 
	
		val conf = inproceedings
			.map{ case(id, item) => item.get("year").toString().toInt}
			.filter(x => x>=1950)
			.map(x => Math.floor(x/10)*10 )
			.map(x => (x.toInt, 1) )
			.reduceByKey( (a, b) => (a + b) )
		
		println("\nQ4a: Conference Papers")
			
		conf.takeOrdered(7).foreach(println)
    }

    /* Q4b.
     * Plot a barchart showing how the average number of collaborators varied in
     * these decades for conference papers in each of the four areas in Q3.
     * Again, the decades will be 1950-1959, 1960-1969, ...., 2000-2009, 2010-2015.
     * But for every decade, there will be four bars one for each area (do not
     * consider UNKNOWN), the height of the bars will denote the average number of
     * collaborators. */
    def q4b(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
		
		//------Calculate (pubkey, (author, decade)) for all papers
	
		val authorshipDecadeArt = article
			.filter(x => x._2.get("year")!=null)
			.map{ case(id, item) => ( id, item.get("year").toString().toInt, item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") ) }
			.map{ case(pubkey, year, authors) => (pubkey, Math.floor(year/10)*10, authors) }
			.map{ case(pubkey, decade, authors) => (pubkey, decade.toInt)->authors }
			.flatMapValues(authors => authors.split(" , "))
			.filter{ case((pubkey, decade), author) => author!="" }	// remove empty author names
			.map{ case((pubkey, decade), author) => pubkey -> (author, decade) }
			
		val authorshipDecadeInp = inproceedings
			.filter(x => x._2.get("year")!=null)
			.map{ case(id, item) => ( id, item.get("year").toString().toInt, item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") ) }
			.map{ case(pubkey, year, authors) => (pubkey, Math.floor(year/10)*10, authors) }
			.map{ case(pubkey, decade, authors) => (pubkey, decade.toInt)->authors }
			.flatMapValues(authors => authors.split(" , "))
			.filter{ case((pubkey, decade), author) => author!="" }	// remove empty author names
			.map{ case((pubkey, decade), author) => pubkey -> (author, decade) }
		
		val authorshipDecade = authorshipDecadeArt.union(authorshipDecadeInp)
		
		
		//------Calculate Authorship: (pubkey, author)
		
		val journAuthorship = article
			.map{ case(id, item) => ( id, item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") ) }
			.flatMapValues(authors => authors.split(" , "))
			
		val confAuthorship = inproceedings
			.map{ case(id, item) => ( id, item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") ) }
			.flatMapValues(authors => authors.split(" , "))
			
		val authorship = journAuthorship.union(confAuthorship) //pubkey, author
			.filter( x => x._2!="")
		
		
		//------Join authorship and authorshipDecade on pubkey to obtain ((author, decade), numOfCollabs)
		
		val numCollabDecade = authorship.join(authorshipDecade) //pubkey, ( author, (author, decade) )
			.map{ case(pubkey, ( author1, (author2, decade) ) ) => (author1, author2, decade) }
			.filter( x => x._1!=x._2 )
			.distinct()
			.map{ case(author1, author2, decade) => (author1, decade) -> 1 }
			.reduceByKey(_ + _)	//author, decade, numOfCollabs
			
		
		//------((author, decade), area) i.e. distinct names of conf paper authors by decade and area
		
		val confDecadeAreaAuthor = inproceedings
			.filter(x => x._2.get("area")!="UNKNOWN")
			.map{ case(id, item) => ( item.get("area"), item.get("year").toString().toInt, item.get("authors").toString().stripPrefix("[ ").stripSuffix("]") ) }
			.map{ case(area, year, authors) => (area, Math.floor(year/10)*10, authors) }
			.map{ case(area, decade, authors) => (area, decade.toInt) -> authors}
			.flatMapValues( authors => authors.split(" , "))
			.distinct()
			.filter{ case((area, decade), author) => author!="" }	// remove empty author names
			.map{ case((area, decade), author) => (author, decade) -> area}	
				
		
		//------Final output: (decade, area, average number of collab-s)
			
		val output = confDecadeAreaAuthor.join(numCollabDecade) //(author, decade),( area, numOfCollabs)
			.map{ case((author, decade), (area, numOfCollabs)) =>  (decade, area) -> numOfCollabs }
			.combineByKey(
				(v) => (v, 1),
				(acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
				(acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)  )
			.map{ case (key, value) => (key, value._1 / value._2.toFloat) }
			.map{ case ((x, y), z) => (x, y.toString(), z)}

		
		println("\nQ4b:")
			
		output.takeOrdered(22)(Ordering[(Int, String)].on(x => (x._1, x._2))).foreach(println)

    }
}


