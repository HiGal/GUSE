# Assignment 1. Search Engine using Hadoop MapReduce
Farit Galeev, Evgenia Kivotova, Kamil Akhmetov, Rinat Babichev

## Contents
1. [ Task description. ](#desc)
2. [ Theory. ](#theory)
3. [The Search Engine description.](#sengine)
4. [ Setting up in Windows. ](#winsetup)
5. [The Search Engine Usage.](#usage)

<a name="desc"></a>
## 1. Task Description

### Goals of the work
Considering multiple documents, it was intended to perform indexing and build search engine so that the top N most relevant documents may be returned based on input query analysis

### Project Structure
```
codimd/
├── dataset/           
├── src/main/java/     
    ├── SearchEngine/
        ├── DocVectorizer.java/    
        ├── Indexer.java/    
        └── Main.java/   
    ├── utils
        ├── ContentExtractor.java
        ├── CustomSerializer.java
        ├── InversedDocumentFrequency.java
        ├── Paths.java
        ├── QueryVectorizer.java
        └── TermFrequency.java
        
```


<a name="theory"></a>
## 2. Theory
### Term Frequency
The Term Frequency (TF) of word in document was calculated as a number of this word occurrences in the document
### Inverse Document Frequency
The Inverse Document Frequency (IDF) of the word was calculated as a number of documents where this word occurs at least one time
### Vector Space Model used
For our implementation it was decided to use sparse vectors for documents representation in next form:

[key1:value1, key2:value2, ..., keyN:valueN]

Where key is some unique word,value is number associated with this word
<a name="sengine"></a>
## 3. The Search Engine description.
Our  Search  Engine’s  structure  may  be  seen  on  the  Picture.   It  consists  of  2  big  components,  IndexEngine and Query engine, which work will be described further in this report
![](https://i.imgur.com/Be6y0Q1.png)

### Index Engine
The Index Engine’s task is to process all input files and compute vector from defined Vector Space Model for each document.  The result of its work will be stored as JSON file in form:

*documentID−title:{word1:value1, word2:value2, ..., wordN:valueN}*

The title of document already is wraped with document id.Index engine consists of 3 parts:  Inversed Document Frequency Counter, Tockens’ Frequency Counter and Document Vectorizer

#### Inversed Document Frequency

Inversed Document Frequency Counter runs MapReduce task for calculating IDF for each unique word. The algorithm is next:

**Map** - For each unique word in the document, write pair (key=word, value=1) to the context.  Do it for every document in the input path.

**Reduce** - Sum all values that was written to the context by Map for one word.  The result will be exact value of IDF for this word.

The  words  and  associated  IDF  values  then  stored  in  the  separate  document  called  ”Vocabulary”, which will be used further in Index Engine as well as in Ranker Engine

#### Term's Frequency Counter

Term’s Frequency Counter runs MapReduce task for calculating TF for each unique word for each document.  The algorithm is next:

**Map** - For each unique word in the document, write pair (key=”word documentID”, value=1) to the context.  Do it for every document in the input path.

**Reduce** - Sum all values that was written to the context by Map for one key = ”word documentID”. The result will be exact value of TF for this word in the document with id = documentID.

The keys and associated TF values then stored in the document as intermediate result to be used further in Index Engine.

#### Document Vectorizer

Document Vectorizer runs MapReduce task for producing vector for each document by uniting the resultsof Inversed Document Frequency Counter and Tockens Frequency Counter.  The algorithm is next:

**Map** - Split each line by TAB symbol and write to context (key=word1, value==word2)
**Reduce** - Combine all words to one document and save as [*documentID : vector*].

### Ranker Engine
Ranker  Engine  uses  the  index  produced  by  Index  Engine  to  analyse  query  and  return  top  N  relevant documents. It consists from 3 parts as well: Query Vectorizer, Relevance Analyser and Content Extractor.

#### Query Vectorizer
Query Vectorizer is the only component that does not use MapReduce.  It splits the input query to words and computes TF for each word (basically, the number of occurrences of word in query).  This value will be called QTF to avoid confusions.  Then it reads words and their IDF from the Vocabulary file produced by Inversed Document Frequency Counter and based on this information represents query as a vector in the next form:

[word1:QFT1/IDF1, word2:QFT2/IDF2, ...]

#### Relevance Analyzer
Relevance Analyser computes the relevance function between the query and each document using MapReduce. The Relevance function in our case is the inner product (scalar product) of document and query vectors.  The algorithm is next:

**Map** - Compute the relevance function between the query and document. The result will be called the Rank of file.  Then, the pair (key = -1*Rank, value = documentID) is writen to context

**Reduce** - Because the Rank value is used as key, docimentIDs will be already sorted, and all we need isto return pair (-1*key, value)

#### Content Extractor
Content Extractor receives file with documentIDs from Relevance Analyser and cuts the top N lines from it.  The title already present in document id in our model.

<a name="winsetup"></a>
## 4. Setting up Hadoop and Java in Windows 
(Ref.: https://www.geeksforgeeks.org/how-to-install-single-node-cluster-hadoop-on-windows/)

#### Download and setup hadoop and Java from official websites

Recommended versions,

Hadoop : hadoop-2.8.0 or hadoop-2.8.1 https://archive.apache.org/dist/hadoop/common/

Java : javac 1.8.0_241 or ..

Step 1: Install Java as usual

Recommended to install in C:\Java\

Step 2.1: Extract Hadoop at C:\hadoop\

Step 2.2: Download Windows binaries for Hadoop versions from https://github.com/steveloughran/winutils

Extract it's contents to C:\hadoop\bin

Step 3: Setting up the HADOOP_HOME variable

Use windows environment variable setting for Hadoop Path setting.

![image](https://user-images.githubusercontent.com/29167718/134938235-02fcc7b0-5098-488e-8f8c-8580e9eeff3c.png)

Step 4: Set JAVA_HOME variable

Use windows environment variable setting for Hadoop Path setting.

![image](https://user-images.githubusercontent.com/29167718/134939563-e823bb31-3a55-4315-8afe-26f65b4961cc.png)

Step 5: Set Hadoop and Java bin directory path

![image](https://user-images.githubusercontent.com/29167718/134939826-ff016f65-e24a-4dbe-8b41-23a7be6bbc50.png)

Step 5.1 Verify the Java and Hadoop installed and setup
```
javac -version
hadoop -version
```

Step 6: Hadoop Configuration :

For Hadoop Configuration we need to modify Six files that are listed below-
```
1. core-site.xml
2. mapred-site.xml
3. hdfs-site.xml
4. yarn-site.xml
5. hadoop-env.cmd
6. Create two folders datanode and namenode inside data folder
   hadoop/data/
              ├── datanode/           
              └── namenode/ 
```

Step 6.1: core-site.xml configuration
```
<configuration>
   <property>
       <name>fs.defaultFS</name>
       <value>hdfs://localhost:9000</value>
   </property>
</configuration>
```

Step 6.2: mapred-site.xml configuration
```
<configuration>
   <property>
       <name>mapreduce.framework.name</name>
       <value>yarn</value>
   </property>
</configuration>
```

Step 6.3: hdfs-site.xml configuration
```
<configuration>
   <property>
       <name>dfs.replication</name>
       <value>1</value>
   </property>
   <property>
       <name>dfs.namenode.name.dir</name>
       <value>C:\hadoop\data\namenode</value>
   </property>
   <property>
       <name>dfs.datanode.data.dir</name>
       <value>C:\hadoop\data\datanode</value>
   </property>
</configuration>
```

Step 6.4: yarn-site.xml configuration
```
<configuration>
   <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
   </property>
   <property>
          <name>yarn.nodemanager.auxservices.mapreduce.shuffle.class</name>  
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
   </property>
</configuration>
```

Step 6.5: hadoop-env.cmd configuration
```
Set "JAVA_HOME=C:\Java" (On C:\java this is path to file jdk)
```

Step 6.6: Create datanode and namenode folders
```
1. Create folder "data" under "C:\hadoop"
2. Create folder "datanode" under "C:\hadoop\data"
3. Create folder "namenode" under "C:\hadoop\data"
```
![image](https://user-images.githubusercontent.com/29167718/134941174-022d7d23-c7db-4951-93a4-79290bfdef61.png)
![image](https://user-images.githubusercontent.com/29167718/134941194-cacef461-e9bf-48ca-837b-d8073103a61d.png)

Step 7.1 Make sure you open command prompt as Administrator

         Start >> Command Prompt >> Right click and select run as administrator

Step 7.2: Format the namenode folder

Open command window (cmd) and typing command 
```
hdfs namenode –format
```
Note: This will delete everything in hdfs, also make sure everything inside data/datanode and data/namenode deleted and are empty before the above command.


### Hadoop and Java installed and ready, now may proceed to run GUSE.jar

Step 1: Navigate to Hadoop directory and then to sbin
```
cd %HADOOP_HOME%
cd sbin
```

Step 2: Start hadoop services
```
start-all
```

Step 3: Create EnWikiSubset directory in hdfs
```
hadoop fs -mkdir /EnWikiSubset
```

Step 4: Transfer the EnWikiSubset files from local to hdfs
```
hadoop fs -put /path/to/GUSE/EnWikiSubset/* /EnWikiSubset
```

Step 5: List the transferred files
```
hadoop fs -ls /EnWikiSubset
```

Step 6.1: Run indexer engine
```
hadoop jar /path/to/GUSE/GUSE.jar Indexer /EnWikiSubset
```

Step 6.2: Run ranker engine, where N = 1, 2, 3, 4, 5, ..
```
hadoop jar /path/to/GUSE/GUSE.jar Query N ”query text”
```

Step 7: Stop hadoop services
```
stop-all
```


<a name="usage"></a>

### The Search Engine Usage
The program is in ”GUSE.jar”.
The next commands may be used to use services of current search engine:

Index Engine:

```java
hadoop jar /path/to/GUSE.jar Indexer /path/to/input/files
```
Where the /path/to/input/files
defines the directory with files that must be indexed

Ranker Engine:
```java
hadoop jar /path/to/GUSE.jar Query N ”query text”
```
Where N defines the maximum number of documents that the query must return
