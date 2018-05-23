▄▄▄▄▄                                                                   ▄▄▄▄▄
    ▀                                                                   ▀
▄▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▄
█ ░ █ --───────────────── ■  Release information   ■ ────────────────-- █ ░ █
▄▄▄▄█ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ █▄▄▄▄
▀                                                                           ▀
▀                                                                           ▀	
							"Sarcasm" Project 
							
Sarcasm project analyzes reviews and categorized them as sarcasm or not according to Stanford NLP program.

Authors:
Amir Avrahami 203204367
Orr Levy 203518766

▄▄▄▄▄                                                                   ▄▄▄▄▄
    ▀                                                                   ▀
▄▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▄
█ ░ █ --─────────── ■ Requirements for Sarcasm Project ■ ────────────-- █ ░ █
▄▄▄▄█ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ █▄▄▄▄
▀                                                                           ▀
▀                                                                           ▀
[ 1 ] JSON formatted input file which includes reviews.
[ 2 ] AWS Credentials file configured in ~.aws.credentials
[ 3 ] Region configured in ~.aws.config
[ 4 ] Requires java 8 to be installed.
[ 5 ] The following queues predefined:
	[a] inputQueue
	[b] outputQueue
	[c] tasksQueue
	[d] resultsQueue

▄▄▄▄▄                                                                   ▄▄▄▄▄
    ▀                                                                   ▀
▄▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▄
█ ░ █ --─────────── ■   IAM Information and Machines   ■ ────────────-- █ ░ █
▄▄▄▄█ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ █▄▄▄▄
▀                                                                           ▀
▀                                                                           ▀
[ 1 ] Workers:
IAM Role: add the following permissions:
	[a] AmazonSQSFullAccess
Image: ami-1582126f
Machine: T2.Medium
*Requires java 8 installed.

[ 2 ] Manager:
IAM Role: add the following permissions:
	[a] AmazonSQSFullAccess
	[b] AmazonS3FullAccess
	[c] AmazonEC2FullAccess
	[d] IAMFullAccess
Image: ami-bdba2ac7
Machine: T2.Micro
*Requires java 8 installed.

▄▄▄▄▄                                                                   ▄▄▄▄▄
    ▀                                                                   ▀
▄▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▄
█ ░ █ --─────────── ■    How to run Sarcasm program    ■ ────────────-- █ ░ █
▄▄▄▄█ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ █▄▄▄▄
▀                                                                           ▀

Run the following command in order to run the Local Application with the necessary information:
java -jar localApp.jar inputFile1 ..... inputFilen n (terminate)
*** terminate is not mandatory, however, if you would wish to tell the manager to terminate and all workers, use it ***

[1] Local Application:
Assuming you have the images as described above, and the correct role, the Local Application will start an instance with 
"Manager" tag, uploads the input file to the correct bucket in s3 and start sending the input files names to InputQueue.
Once it has finished to upload all input files, the local applications listens to outputQueue and waits for a message that
the files have finished  processing and ready to be downloaded.
Afterwards, local application will read the results file and will turn it into an HTML file.

[2] Manager:
The manager waits for messages in inputQueue, once it receives a message it performs the following actions:
	[a] Downloading the file from S3
	[b] Parsing it into Review objects
	[c] Sending each review as a message to taskQueue
Also, the manager is responsible for initiating workers according to a given n value.
Once the workers are done, all the tasks are submitted to resultsQueue, the manager receives each message from
resultQueue and performs the following:
	[a] Creates an output file matching the name of the input file
	[b] Writes to the output file the relevant reviews
	[c] Uploads the output file to S3
	[d] Sends a message to OutputQueue with the filename
If a local application sent a "terminate" message, the manager will terminate all workers and itself.
	
[3] Worker:
The Worker is reponsible for getting a task (review) from tasksQueue, parsing it and running sentiment and entities analysis 
(using Stanford NLP).
When the analysis is done, the worker sends a message which includes the review, sentiment and entities to resultsQueue.

▄▄▄▄▄                                                                   ▄▄▄▄▄
    ▀                                                                   ▀
▄▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▄
█ ░ █ --─────────── ■         Running results          ■ ────────────-- █ ░ █
▄▄▄▄█ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ █▄▄▄▄
▀                                                                           ▀
Input files: 5 input files
	[1] 0689835604
	[2] B000EVOSE4
	[3] B001DZTJRQ
	[4] B0047E0EII
	[5] B01LYRCIPG

With filter: message < 100 chars
n Value: 100
Time to finish: 130.544 seconds

Without filter:
n Value: 100
Time to finish: 314.339 seconds

▄▄▄▄▄                                                                   ▄▄▄▄▄
    ▀                                                                   ▀
▄▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▄
█ ░ █ --─────────── ■       Questions & Answers        ■ ────────────-- █ ░ █
▄▄▄▄█ ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ █▄▄▄▄
▀                                                                           ▀

Q: Did you think for more than 2 minutes about security?
A: Yes, credentials to local application are provided via configuration files and the manager and workers are provided
with credentials through an IAM role.

-------------------------------------------------------------------------------------------------------------------------------

Q: Did you think about scalability? 
Will your program work properly when 1 million clients connected at the same time? How about 2 million? 1 billion? 
A: Our implementation is such that at any given moment there are possibly up to 2 input files loaded to manager's memory (could
be less) which means that it doesn't grow as number of clients grow larger.
In addition, we save a table which includes filenames and number of reviews per each file which does grow as number
of clients grow, however each file eventually removed from the table as soon as the manager finish handling it.
To sum up, our program is quite scalable however, we made some compromises for run time considerations.

-------------------------------------------------------------------------------------------------------------------------------

Q: What about persistence? What if a node dies? What if a node stalls for a while? 
Have you taken care of all possible outcomes in the system? What did you do to solve it?
A: Our program works such that persistence issues are handled via AWS SQS configurations:
Our tasksQueue is configured to have long invisibility time for two reasons:
	[a] In case a worker dies during processing a task, after the invisibility period is over, another worker
	can continue working on this task.
	[b] In case a worker stalls, the long invisibility period make sure that no other worker will work on the same task
	which will cause conflicts.
	
-------------------------------------------------------------------------------------------------------------------------------

Q: What about broken communications?
A: We've handled communication fail cases to our best, however, some fail cases such as a message that wasn't sent properly
to the queue from a worker to the manager cannot be handled by our business program rather than Amazon's.

-------------------------------------------------------------------------------------------------------------------------------

Q: Threads in your application, when is it a good idea? When is it bad?
A: We used Threads for the manager logic business since a few logics should be happening at the same time:
	[a] Receiving messages from InputQueue
	[b] Parsing input files
	[c] Initiating workers
	[d] Receiving messages from resultQueue
	[e] Writing results to output file
These logics cannot be dependent on each other and should be happening simultaneously, hence must be parallel via threads.
We didn't use threads in worker or local app as it wasn't necessary.

-------------------------------------------------------------------------------------------------------------------------------
	
Q: Did you run more than one client at the same time?
A: Yes, we did, we made some tests by running three local applications at once.

-------------------------------------------------------------------------------------------------------------------------------

Q: Did you manage the termination process?
A: Once the manager receives termination message in inputQueue, it's waiting for all current tasks to finish and
terminates both workers instances and itself.

-------------------------------------------------------------------------------------------------------------------------------

Q: Did you take in mind the system limitations that we are using?
A: We did take the system limitations in mind for example:
	[1] The workers are using Stanford CoreNLP which is a memory consumer - therefore we used T2.Medium machine which
	has sufficient memory to run the tool.
	[2] It is common to use thread pool of size 4 on multicore machines, since we use T2.micro which has a single core,
	we adjusted the thread pool size to use 2 threads. (Performance tests showed higher performance)

-------------------------------------------------------------------------------------------------------------------------------

Q: Are all your workers working hard? Or some are slacking? Why?
A: We are initiating the workers in waves of 5 seconds interval, which causes the very first workers to process more tasks
than the newly created workers.
Moreover, there are some tasks which take longer to process than others, which also causes to the workers processing them
to handle less tasks.

-------------------------------------------------------------------------------------------------------------------------------

Q: Is your manager doing more work than he's supposed to? Have you made sure each part of your system has properly defined tasks?
A: Our manager does exactly what it is supposed to do according to the instructions.
Moreover, each thread of the manager process has a defined purpose.

-------------------------------------------------------------------------------------------------------------------------------

Q: Are you sure you understand what distributed means? Is there anything in your system awaiting another?
A: Yes, we do believe we understand the term "distributed". The distributed part of our program is the workers processing
tasks simultaneously and separately, undependent of each other.
The only bottle neck in our program is the local applications waiting for the output files to be uploaded from the manager.
