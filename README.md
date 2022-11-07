# Project1  Distributed File System (v 1.0)

### About the project
Refer to the course web page: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-1.html

<ul>
    <li><b>Parallel</b> retrievals: large files will be split into multiple chunks. Client applications retrieve these chunks in parallel using goroutines.</li>
    <li><b>Replication</b>:each file will be replicated to provide a level of fault tolerance.</li>
    <li><b>Interoperability</b>: the DFS will use Google Protocol Buffers to serialize messages. This allows other applications to easily implement your wire format.</li>
    <li><b>Persistence</b>: once a file has been acknowledged as stored, you should be able to terminate the entire DFS, restart it, and still have access to the stored files.</li>
</ul>

### Features

#### Controller
<code>Controller</code> has the information of all <code>Storage Nodes</code>. The <code>Controller</code> listens the incoming requests sent by clients, if the controller receives a <code>put</code>the tracker will responds with a list of all storage nodes that storing the chunks of file. Then the client will send requests to <code>Storage Node</code> for storing chunks of file.
![](https://i.imgur.com/XgdZeqW.png)

#### Storage Node
<code>Storage Nodes</code> are responsible for storing and retriving file chunks.

The <code>Storage Node</code> periodically sends a <b>hearbeat</b> message to <code>Controller</code>, so the controller will know the storage is alive. The <b>hearbeat</b> message also contains the free space and number of requests that the storage node has handled.

#### Client
<ul>
    <li><b>Put</b></li> Breaking files into chunks and get a list of storage nodes that will store them from the <code>controller</code>, then sending chunks to storage nodes.
    <li><b>Get</b></li> Retriving files. Send file name and folder name to <code>controller</code>, then controller will let the client know which storage node has the file. The client will retrieve chunks in parallel.
    <li><b>Delete</b></li> Sending deleting message to controller, controller will delete chunks.
    <li><b>Ls</b></li> Listing files 
    <li><b>Storage status</b></li> Get the number of requests and free space of each storage node.
</ul>

![](https://i.imgur.com/zqYsFUD.png)


![](https://i.imgur.com/q03na8d.png)

![](https://i.imgur.com/OBx76iN.png)

### Demo

Simple tests on localhsot.

Controller: host:"localhost", port:9999
Storage node 1: id:1 host:"localhost", port:1111
Storage node 2: id:2 host:"localhost", port:2222
Storage node 3: id:3 host:"localhost", port:3333
Client 1: id:7 host:"localhost", port:7777
Client 2: id:8 host:"localhost", port:8888

Command used to start the system:

We are assuming that the storage nodes and clients know the position of the controller.

<ul>
    <li>Start controller</li>
    <pre><code>go run controller/controller.go</code></pre>
    <li>Start storage nodes</li>
    <ul>
        <li>Start sn1
            <pre><code>go run storageNode/storageNode.go 1 localhost 1111 /Users/liuli/Desktop/storage/1111</code></pre>
    id: 1 <br>
    host: "localhost"<br>
    port: 1111<br>
    path: /Users/liuli/Desktop/storage/1111 
        <li>Start sn2</li>
    <pre><code>go run storageNode/storageNode.go 2 localhost 2222 /Users/liuli/Desktop/storage/2222</code></pre>
    id: 2 <br>
    host: "localhost"<br>
    port: 2222<br>
    path: /Users/liuli/Desktop/storage/2222
    <li>Start sn3</li>
    <pre><code>go run storageNode/storageNode.go 3 localhost 3333 /Users/liuli/Desktop/storage/2222</code></pre>
    id: 3 <br>
    host: "localhost"<br>
    port: 3333<br>
    path: /Users/liuli/Desktop/storage/3333
    </ul>
    <li>Start clients</li>
    <ul>
        <li>client1</li>
        <pre><code>go run client/client.go 7 localhost 7777</code></pre>
        client_id: 7
        client_host: "localhost"
        client_port: 7777
        <li>client1</li>
        <pre><code>go run client/client.go 8 localhost 8888</code></pre>
        client_id: 8
        client_host: "localhost"
        client_port: 8888
    </ul>

</ul>

#### Put

command: <code>put /Users/liuli/Desktop/Big_Data/P1-crackdfs/test.txt /work</code>

<img src="https://i.imgur.com/t45a6dp.png" width="400" height = "200"/>

<img src="https://i.imgur.com/9Qgq0E2.png" width="600" height = "200"/>

under directory "/Users/liuli/Desktop/storage/node_id/work", there are four chunks of files.

#### Get

command: <code>get test.txt</code>

<pre><code>return_chunk_message:{chunk:"onstruct files from the blocks spread across the datanodes. To mitigate this risk, it is possible to"  id:1}
return_chunk_message:{chunk:"he deployment. In our case, we can get by without high availability."  id:3}
return_chunk_message:{chunk:" run a hot standby namenode for high availability depending on the fault tolerance requirements of t"  id:2}
return_chunk_message:{chunk:"The namenode is a single point of failure: if it is lost, then there is no way of knowing how to rec"}
2022/10/07 15:53:27 wrote to file: test.txt, len: 368</code></pre>

log shows 4 chunks have been saved to file <code>test.txt</code>

#### Delete

command: <code>delete test.txt /work</code>
<code>/work</code> is the directory

#### GetMap

command: <code>getMap</code>
<pre><code>Id of storage node: 1 => Node_info: id:1  host:"localhost"  port:1111  free_space:261776  number_of_requests:4
Id of storage node: 3 => Node_info: id:3  host:"localhost"  port:3333  free_space:261776  number_of_requests:4
Id of storage node: 2 => Node_info: id:2  host:"localhost"  port:2222  free_space:261776  number_of_requests:4
</code></pre>

#### Ls
command: <code>ls</code>
<pre><code>-work--*test.txt</code></pre>

#### Tests on Orion
<pre><code>Storage Nodes:
go run storageNode/storageNode.go 2 orion02 22002 /bigdata/lliu78/storage/2222

go run storageNode/storageNode.go 3 orion03 22003 /bigdata/lliu78/storage/3333
go run storageNode/storageNode.go 4 orion04 22004 /bigdata/lliu78/storage/4444
go run storageNode/storageNode.go 5 orion05 22005 /bigdata/lliu78/storage/5555

Controller: orion01
go run controller/controller.go

Clients:
go run client/client.go 11 orion11 22011
-> put test_file_3.bin
-> getMap

go run client/client.go 12 orion12 22012
-> get test_file_3.bin
-> ls
-> delete test_file_3.bin</code></pre>

Please see p1_test.txt for testing insturctions!!
