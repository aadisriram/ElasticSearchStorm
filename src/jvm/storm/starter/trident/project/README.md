
Part I: Download, install, and run ElasticSearch (ES)

	Download ElasticSearch from here (check latest version). 
		http://www.elasticsearch.org/overview/elkdownloads/
		
  0. cd $HOME
  1. Download tar ball: 
	 wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.2.tar.gz
  2. gunzip elasticsearch-1.4.2.tar.gz
  3. tar xvf elasticsearch-1.4.2.tar
  4. cd $HOME/elasticsearch-1.4.2.tar
  5. Run the ES server: ./bin/elasticsearh 
     or run in the background but save PID in a file, pid_es.txt, 
	 so that the daemon could be killed later by PID:
	 kill -9 PID:

     ./bin/elasticsearch -d -p pid_es.txt

  6. Test the ES server: curl -X GET http://localhost:9200/
		You will get an output that looks like this:
		{
		  "status" : 200,
		  "name" : "Sagittarius",
		  "cluster_name" : "elasticsearch",
		  "version" : {
			"number" : "1.4.2",
			"build_hash" : "927caff6f05403e936c20bf4529f144f0c89fd8c",
			"build_timestamp" : "2014-12-16T14:11:12Z",
			"build_snapshot" : false,
			"lucene_version" : "4.10.2"
		  },
		  "tagline" : "You Know, for Search"
		}


Part II: Get familiar with ElasticSearch capabilities
	
	Make sure that ES is running (see Part I) before testing 
	the commands below and/or running Storm/Trident application!!!
	
	Ideally, you should go over a short tutorial on ES web-site;
	at a minimum the "getting started" section of the definitive guide:
	http://www.elasticsearch.com/guide/en/elasticsearch/guide/current/index.html
	
	NOTE: In general to interface with ES (NOT within Java code), 
	use the following syntax:
	
	curl -X<REST Verb> <Node>:<Port>/<Index>/<Type>/<ID>
	
II.A. Create index of a particular type and populate/update with a record(s)
	
	curl -XPUT 'localhost:9200/customer_index/external_type'
	
	curl -XPUT 'localhost:9200/my_index/my_type/1?pretty' -d '
	{
		"name": "John Doe"
	}'

	curl -XPOST 'localhost:9200/my_index/my_type/1/_update?pretty' -d '
	{
		"doc": { "name": "Jane Doe", "age": 20 }
	}'
	
	curl -XPOST 'localhost:9200/customer_index/external_type/_bulk?pretty' -d '
		{"index":{"_id":"1"}}
		{"name": "John Doe" }
		{"index":{"_id":"2"}}
		{"name": "Jane Doe" }
	'
II.B. Query index
	
	curl 'localhost:9200/customer_index/_search?q=*&pretty'
	
	curl -XDELETE 'localhost:9200/my_index/my_type/_query?pretty' -d '
	{
		"query": { "match": { "name": "Jane" } }
	}'

	curl -XPOST 'localhost:9200/my_index/_search?pretty' -d '
	{
		"query": { "match_all": {} }
	}'

II.C. Delete index or record
	
	curl -XDELETE 'localhost:9200/my_index'
	curl -XDELETE 'localhost:9200/my_index/my_type/1?pretty'
	curl -XDELETE 'localhost:9200/my_index/my_type/_query?pretty' -d '
	{
		"query": { "match": { "name": "John" } }
	}'
	
II.D. Check ES status, indicies, index types, etc
	
	curl 'localhost:9200/_cat/indices?v'
	curl 'localhost:9200/_cat/health?v'
	curl 'localhost:9200/_cat/nodes?v'
	curl ‘http://127.0.0.1:9200/my_index/_mapping?pretty=1’

Part III: Consulting with ES APIs (including Java APIs to ES)

    http://www.elasticsearch.com/guide/index.html
	
	Other useful links:
	http://www.elasticsearch.org/
	https://github.com/elasticsearch/elasticsearch
	http://mvnrepository.com/artifact/org.elasticsearch/elasticsearch
	http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/query-dsl-queries.html

Part IV: VERY Useful Java testing framework for ES:

	https://github.com/tlrx/elasticsearch-test/blob/master/src/test/java/com/github/tlrx/elasticsearch/test/EsSetupTest.java

Part V: Trident library for ElasticSearch to be used in pom.xml

   https://github.com/fhussonnois/storm-trident-elasticsearch

