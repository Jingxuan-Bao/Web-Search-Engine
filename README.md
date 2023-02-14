# Web Search Engine

#### Group Name on Canvas: Strugle

#### Group Members:

- Jingxuan Bao (bjx@seas.upenn.edu)
- Qihang Dai (ahgdyycc@seas.upenn.edu)
- Yanzhi Wu (yanzhiwu@seas.upenn.edu)
- Yuanhong Xiao (yuanhong@seas.upenn.edu)

#### Sample:
![Image text](https://github.com/Jingxuan-Bao/Web-Search-Engine/blob/b17b75fc7f067d68296dc0fdffbad8a89e8c4bb2/image/sample1.png)
![Image text](https://github.com/Jingxuan-Bao/Web-Search-Engine/blob/b17b75fc7f067d68296dc0fdffbad8a89e8c4bb2/image/sample2.png)
#### To run each component locally:

- First, install all dependencies needed for each component.

- Indexer:

```
Run Indexer.java
```

- PageRank:

```
1. Run RdsRead.java (to read the link table of rds).

2. Run PageRank.java (to calculate the pagerank result).

3. Run RdsWrite.java (to write the pagerank result into the pagerank table of rds).
```

- Search Engine:

```
Run Server.java
```

- User Interface:

```
Run the command npm start
```

- Distributed Web Crawler:

```
1. Start crawler master server (MasterServer.java). Don't shutdown the master server until everything is done.
You can just type "mvn exec:java@master" in the command line to start the master server.
You should provide 1 argument:
{Master Server Port NUmber} (mvn default value: 45555)

2. Start multiple crawler worker servers (WorkerServer.java) for crawling tasks.
You can just type "mvn exec:java@worker1-crawl" and "mvn exec:java@worker2-crawl" in the command line to start two worker servers.
You should provide 7 arguments:
{Master Server Address} (mvn default value: localhost:45555)
{Worker Server Port Number} (mvn default value: 8001, 8002)
{Storage Directory} (mvn default value: ./storage/worker1, ./storage/worker2)
{Max Document Size in MB} (mvn default value: 3)
{Max URL Length} (mvn default value: 300)
{Number of DocumentFetcherBolt Executors} (mvn default value: 3)
{Number of LinkExtractorBolt Executors} (mvn default value: 3)

3. You can submit a new crawling task by providing {Start URL} and {Number of Documents to Crawl} on the home page and hit {Start}.
You can do this as many times as you want.

4. You can refresh the home page to monitor the crawling information.

5. After enough documents have been crawled or during the crawling process, you can hit {Stop Crawling} to stop crawling.
You cannot restart crawling after that unless you restart worker servers.

6. Don't worry to restart worker servers to restart crawling because data are kept permanently on disk.

7. Shutdown crawler worker servers and start ONE worker server at a time to upload crawled documents to the remote AWS RDS.
You can just type "mvn exec:java@worker1-upload" and "mvn exec:java@worker2-upload" in the command line to start ONE worker server at a time.
You should provide 8 arguments:
{Master Server Address} (mvn default value: localhost:45555)
{Worker Server Port Number} (mvn default value: 8001, 8002)
{Storage Directory} (mvn default value: ./storage/worker1, ./storage/worker2)
{RDS DB Name} (mvn default value: cis555db)
{RDS DB Username} (mvn default value: admin)
{RDS DB Password} (mvn default value: cis45555)
{RDS DB Documents Table Name} (mvn default value: Documents3)
{RDS DB Links Table Name} (mvn default value: Links3)

8. Once you start a worker server to upload crawled documents, you can monitor the uploading information on the home page.

9. The uploading may stop due to Internet connection issue or AWS issue.
You can safely restart the worker server to continue uploading. The worker will skip uploaded documents automatically.

10. You can interleave crawling and uploading because all data are kept permanently on disk.
```
