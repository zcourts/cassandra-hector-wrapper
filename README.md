**Simple wrapper**


This a simple wrapper I wrote for Hector.
-----------------------------------------

It doesn't support all the features. The main point was to get something quick and simple.
I did this on the train over 3/4 mornings while heading to work.

I'll review it and make some needed changes however it does currently work fine.

Usage is similar to Hector's see https://github.com/rantav/hector if you want to use Hector directly.
See the file https://github.com/zcourts/cassandra-hector-wrapper/blob/master/src/main/java/com/scriptandscroll/adt/UsageExamples.java
for a decent set of usage examples.

You start by creating a Keyspace object.


```java

Keyspace ks=new Keyspace("clusterName", "keyspaceName", "localhost:9160") ;

//then a column or super column family object
ColumnFamily cf= new ColumnFamily(ks,"columnFamilyName");

//now the magic happens, you simple do cf.get[column|columns|row,rows]
Row row= cf.Row getRow("rowKey", "startColumn", "endColumn");

//you can now do
Column col = row.getColumn("columnName");
//then
String val= col.getValue();
//  OR .....
String val2=row.getColumnValue("columnName");
//OR
Iterator<Column> it=row.iterator();
while(it.hasNext()){
  Column c = it.next();
  //do whatever
}


//Thats it!

```


Its important to note that I didn't write this because the Hector client was lacking in anyway at all.
Quite the opposite in fact. The guys working on hector have done an awesome job and myself and I'm sure others
appreciate it. However when I was working on updating a project recently it was taking me far too much time to sift
through the hector docs and get familiar with all the changes etc. I started with a single file but that quickly got too nasty
and I just stopped, drew out some ideas and it turned out into all the classes currently in this repo.