package scheduler;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.*;

import common.*;

public class Scheduler {

  	int schedulerPort;
  	Cluster cluster;
  	int jobIdNext;
  	int numConnections;
  	
	Scheduler(int p) {
    		schedulerPort = p;
    		cluster = new Cluster();
    		jobIdNext = 1;
    		numConnections = 0;
  	}

  	public static void main(String[] args) {
    		Scheduler scheduler = new Scheduler(Integer.parseInt(args[0]));
    		scheduler.startServer();
  	}

  	public void startServer() {
		try{
      			//create a ServerSocket listening at specified port
      			ServerSocket serverSocket = new ServerSocket(schedulerPort);
    			//System.out.println( "scheduler socket started\n");
			JobsScheduler jobsscheduler = new JobsScheduler();
			new Thread(jobsscheduler).start();
   			while(true){
    				//accept connection from worker or client
        			Socket socket = serverSocket.accept();
				numConnections ++;
				//System.out.println( "connection accepted"+numConnections);
				ParallelSocket socket1 = new ParallelSocket(socket, numConnections, this, cluster, jobsscheduler);
				new Thread(socket1).start();
			}
      		}
      		catch(Exception e) {
        		e.printStackTrace();
      		}
  	}

	class JobsScheduler implements Runnable {
		LinkedList<Job> JobsToSchedule;
		LinkedList<Job> RunningList;
		int total_workers;
		int num_workers;
	
		public JobsScheduler() {
			JobsToSchedule = new LinkedList<Job>();
			RunningList = new LinkedList<Job>();
			total_workers = cluster.numTotalWorkers();
			num_workers = 0;
		}

		void addRunningList(Job job)
		{
			synchronized(RunningList)
			{
				RunningList.add(job);
			}
		}

		boolean removeRunningList(Job job)
		{
			boolean success = false;
			synchronized(RunningList)
			{
					success = RunningList.remove(job);
					//System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&peek first "+RunningList.peekFirst());
			}
			return success;
		}

		Job getFirstRunningList(Job myJob) {
			Job job = null;
			synchronized(RunningList)
			{
				if(RunningList.size()>0)
				{
					job = RunningList.peekFirst();			
					if(job != null)
					{
						if(job.jobID != myJob.jobID)
							RunningList.remove(job);
						else if(RunningList.size()>1)
							job = RunningList.remove(1);						
						else
							job = null;
						if(job != null)
						{
							System.out.println("****************killing job "+job.jobID);
						}
					}
				}
			}
			return job;
		}

		void addJob(Job job)
		{
			synchronized(JobsToSchedule) {
				if(job.failedJob == true)
				{
					JobsToSchedule.addFirst(job); 
					System.out.println("Added failed job: "+job.jobID);
				}	
				else
				{	
					JobsToSchedule.add(job); 
					System.out.println("Added job: "+job.jobID);
				}
			}
		}
	
	
		void removeJob(Job job, int n) {
			synchronized(JobsToSchedule) {
				if(job!=null)
				{
					System.out.println("Removing job: "+job.jobID);
					JobsToSchedule.remove(job);
				}				
				num_workers += n;
			}
		}
		
		public void run() {
			int time = 100;
			while(true){ //infinite loop
				synchronized (JobsToSchedule)
				{
					int updated_workers = cluster.numTotalWorkers();
					if(updated_workers>total_workers) {
						num_workers += (updated_workers-total_workers);	
						total_workers = updated_workers;			
					}
					else if(updated_workers<total_workers) {
						num_workers -= (total_workers-updated_workers);
						total_workers = updated_workers;
					}
					int size = JobsToSchedule.size();
					int numWorkers = num_workers;
				//	System.out.println("updated_workers: "+updated_workers);
				//	System.out.println("total_workers: "+total_workers);
				//	System.out.println("num_workers: "+num_workers);
				//	if(size != 0 && numWorkers !=0) {
					if(size != 0) {
						int dmin = Integer.MAX_VALUE;
        	   				ListIterator<Job> itr = JobsToSchedule.listIterator();
						boolean all_scheduled = true; 
    					//	while( itr.hasNext() && numWorkers>0) {
						while(itr.hasNext()) {
							Job job = itr.next();
							if(!job.scheduled)
							{
								//System.out.println("Scheduling jobs. jobs to schedule = "+size+" workers available = "+numWorkers);
								if(numWorkers>0) 
								{
									all_scheduled = false;
									numWorkers--;
									//job.scheduled = true;
									job.numWorkers = 1;
									if(job.numTasks<dmin && job.numTasks != 1 && job.failedJob == false)
									{
										dmin = job.numTasks;
									}
								}
								else if(!job.killedJob)  //kill a task and schedule new one 
								{
									Job jobToKill = getFirstRunningList(job);
									if(jobToKill != null) {
										jobToKill.kill = true;
										all_scheduled = false;
										numWorkers = jobToKill.numWorkers - 1;
										num_workers = numWorkers;
										//job.scheduled = true;
										job.numWorkers = 1;
										if(job.numTasks<dmin && job.numTasks != 1 && job.failedJob == false)												
										{
											dmin = job.numTasks;
										}
									}
								}
							}
						}
					//	System.out.println("All_scheduled: "+all_scheduled); 
						if(all_scheduled)
							time+=100;
						else
						{
							time=100;
								ListIterator<Job> itr2 = JobsToSchedule.listIterator();
								int dsh = dmin;						
								dmin = Integer.MAX_VALUE;
    								while( itr2.hasNext() && numWorkers>0){
									Job job = itr2.next();
									if(!job.scheduled)
									{
									if(job.numTasks == dsh && job.failedJob == false)
									{
										for(int i=job.numTasks; i>1 && numWorkers>0; i--, numWorkers--)
										{
											job.numWorkers++;
										}
									}
									else if(job.numTasks>dsh && job.numTasks<dmin && job.failedJob == false)	
									{
										dmin = job.numTasks;
									}
									}
								}
							ListIterator<Job> itr1 = JobsToSchedule.listIterator();
    							while( itr1.hasNext()){
								Job job = itr1.next();
								if(!job.scheduled && job.numWorkers>0)
								{
									job.done_scheduling=true;
									job.scheduled=true;
									System.out.println("Done scheduling job "+job.jobID);
									//Job newjob = new Job(job);
									if(!job.failedJob && !job.killedJob)
										addRunningList(job);
								}
							}
							num_workers = numWorkers;
						}
        	  			}
					else
						time+=100;
				}
				try {
					Thread.sleep(time);
				}
				catch(Exception e) {
					System.out.println(e);
				}
			}
		}
	}	


	class ParallelSocket implements Runnable {
    		DataInputStream dis;
    		DataOutputStream dos;
    		Socket socket;
    		int id;
    		Scheduler server;
    		JobsScheduler jobsscheduler;
		int failNum;
		int numWorkers;
		int numThreads;
		int taskfailed;

    		public ParallelSocket(Socket socket, int id, Scheduler server, Cluster cluster, JobsScheduler jobsscheduler) {
			this.socket = socket; 
			this.id = id;
			this.server = server; 
			this.jobsscheduler = jobsscheduler;
			failNum = 0;
			taskfailed=0;
    		}

  		public void run() {
    			try{
				//System.out.println( "in thread "+id);
        			DataInputStream dis = new DataInputStream(socket.getInputStream());
        			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        			int code = dis.readInt();

        			//a connection from worker reporting itself
        			if(code == Opcode.new_worker){
          				//include the worker into the cluster
          				WorkerNode n = cluster.createWorkerNode( dis.readUTF(), dis.readInt());
          				if( n == null){
            				dos.writeInt(Opcode.error);
          				}
          				else{
            				dos.writeInt(Opcode.success);
            				dos.writeInt(n.id);
            				System.out.println("Worker "+n.id+" "+n.addr+" "+n.port+" created");
          				}
          				dos.flush();
        			}

        			//a connection from client submitting a job
        			if(code == Opcode.new_job){
          				String className = dis.readUTF();
          				long len = dis.readLong();

          				//send out the jobId
          				int jobId = server.jobIdNext++;
          				dos.writeInt(jobId);
          				dos.flush();

          				//receive the job file and store it to the shared filesystem
          				String fileName = new String("fs/."+jobId+".jar");
          				FileOutputStream fos = new FileOutputStream(fileName);
          				int count;
          				byte[] buf = new byte[65536];
          				while(len > 0) {
            					count = dis.read(buf);
            					if(count > 0){
              						fos.write(buf, 0, count);
              						len -= count;
            					}
          				}
          				fos.flush();
          				fos.close();
          
					//get the tasks
          				int taskIdStart = 0;
          				int numTasks = JobFactory.getJob(fileName, className).getNumTasks();

	  				Job myJob = new Job(jobId, className, numTasks, 0, false, false);
	  				jobsscheduler.addJob(myJob);
	  				while(myJob.done_scheduling==false)
					{
						System.out.print("");
					}

	  				System.out.println("Job scheduled "+myJob.jobID+" workers assigned: "+myJob.numWorkers);
					numWorkers = myJob.numWorkers;
	
					int i;
					int tasksAssigned = numTasks / numWorkers;
					int task_left=0, start_task=0;
					//notify the client
          				dos.writeInt(Opcode.job_start);
          				dos.flush();
					for(i=0; i<numWorkers-1; i++)
					{
						task_left = tasksAssigned;
						start_task = i*tasksAssigned;
						numThreads++;
						ScheduleWorker work = new ScheduleWorker(this, myJob, task_left, start_task, dos, jobsscheduler);
						new Thread(work).start();
					}
					task_left = tasksAssigned + (numTasks % numWorkers);
					start_task = i*tasksAssigned;
					numThreads++;
					ScheduleWorker work = new ScheduleWorker(this, myJob, task_left, start_task, dos, jobsscheduler);
					new Thread(work).start();

				//	System.out.println("numThreads "+numThreads);
					while(numThreads>0) 
					{
						System.out.print("");
					}
					boolean success=true;;
					if(myJob.kill==false)
					{
						success = jobsscheduler.removeRunningList(myJob);
						System.out.println("*****************removed job "+myJob.jobID+ "success= "+success);
						jobsscheduler.removeJob(myJob, numWorkers);
					}
					else
					{
						int failed = 0 - taskfailed;
						jobsscheduler.removeJob(myJob, failed);
					}
					
						

					System.out.println("failNum "+failNum);
					while(failNum!=0)
					{
						System.out.print("");
					}
					//notify the client
					synchronized(dos){
          				dos.writeInt(Opcode.job_finish);
          				dos.flush();
					}
				}

        			dis.close();
        			dos.close();
        			socket.close();
    			} 
			catch(Exception e) {
      				e.printStackTrace();
    			}
  		}
	}

	class ScheduleWorker implements Runnable {
		Job myJob; 
		int task_left; 
		int start_task;
		DataOutputStream dos;
		ParallelSocket socket;

		public ScheduleWorker(ParallelSocket socket, Job myJob, int task_left, int start_task, DataOutputStream dos, JobsScheduler jobsscheduler) {
			this.socket = socket;			
			this.myJob = myJob;
			this.task_left = task_left;
			this.start_task = start_task; 
			this.dos = dos;
		}
	
		public void run() {
			try {
				//get a free worker
          			WorkerNode n = cluster.getFreeWorkerNode();
          			System.out.println(task_left+" tasks assinged to worker "+n.id);
				//assign the tasks to the worker
        			Socket workerSocket = new Socket(n.addr, n.port);
          			DataInputStream wis = new DataInputStream(workerSocket.getInputStream());
          			DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());
          
          			wos.writeInt(Opcode.new_tasks);
          			wos.writeInt(myJob.jobID);
          			wos.writeUTF(myJob.className);
				wos.writeInt(start_task);
          			wos.writeInt(task_left);
          			wos.flush();

				boolean killed = false;
				//repeatedly process the worker's feedback
          			while(wis.readInt() == Opcode.task_finish) {
					task_left--;
					start_task++;
					synchronized(dos)
					{
            			 		dos.writeInt(Opcode.job_print);
            					dos.writeUTF("task "+wis.readInt()+" finished on worker "+n.id);
            					dos.flush();
					}
					if(myJob.kill==true)
					{
						if(task_left>0)
							wos.writeInt(0);
						else 
							wos.writeInt(1);
						killed = true;
						break;
					}
					else
						wos.writeInt(1);
          			}

          			//disconnect and free the worker
          			wis.close();
          			wos.close();
          			workerSocket.close();
          			cluster.addFreeWorkerNode(n);	
				if(killed==true)
				{
					//jobsscheduler.removeJob(null, 1);
					synchronized(dos)
					{
						if(task_left>0)
						{
							socket.failNum++;
							Job failJob = new Job(myJob.jobID, myJob.className, task_left, start_task, false, true);
							FaultTolerance faultcase = new FaultTolerance(failJob, dos, socket);
							new Thread(faultcase).start();
						}	
						socket.numThreads--;
					}
				}
				else
				{
					synchronized(dos){
						socket.numThreads--;
					}
				}
			}
			catch (Exception e) {
				System.out.println("///////////////////////////////////////scheduler");
				e.printStackTrace();
				Job failJob = new Job(myJob.jobID, myJob.className, task_left, start_task, true, false);
				FaultTolerance faultcase = new FaultTolerance(failJob, dos, socket);
				new Thread(faultcase).start();
				synchronized(dos)
				{
					socket.taskfailed++;
					socket.failNum++;
					socket.numWorkers--;
					socket.numThreads--;
				}
			}
		}
	}	

	class FaultTolerance implements Runnable {
		Job myJob;
 		DataOutputStream dos;
		ParallelSocket socket;
	
		public FaultTolerance(Job myJob, DataOutputStream dos, ParallelSocket socket) {
			this.myJob = myJob;
			this.dos = dos;
			this.socket = socket;
		}
		
		public void run() {
			boolean fail = false;
			do
			{
				if(fail == true) 
					myJob.done_scheduling = false;
				else			
					socket.jobsscheduler.addJob(myJob);
	  			while(myJob.done_scheduling==false)
					System.out.print("");

	  			System.out.println("Job scheduled "+myJob.jobID);
				int numWorkers = myJob.numWorkers;
				try {
					//get a free worker
        	  			WorkerNode n = cluster.getFreeWorkerNode();
					System.out.println(myJob.numTasks+" tasks assinged to worker "+n.id);
        	  			Socket workerSocket = new Socket(n.addr, n.port);
	          			DataInputStream wis = new DataInputStream(workerSocket.getInputStream());
	          			DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());
          
	          			wos.writeInt(Opcode.new_tasks);
	          			wos.writeInt(myJob.jobID);
	          			wos.writeUTF(myJob.className);
	     				wos.writeInt(myJob.taskIdStart);
	     				wos.writeInt(myJob.numTasks);
	          			wos.flush();

					//repeatedly process the worker's feedback
	          			while(wis.readInt() == Opcode.task_finish) {
						int taskId = wis.readInt();
						System.out.println("/////////////////////////////////task "+taskId+" finished on worker "+n.id);
						try
						{
							synchronized(dos)
							{
				            			dos.writeInt(Opcode.job_print);
				            			dos.writeUTF("task "+taskId+" finished on worker "+n.id);
				            			dos.flush();
								wos.writeInt(1);
							}
						}
						catch(Exception e) {							
						}
          				}
          				//disconnect and free the worker
          				wis.close();
          				wos.close();
          				workerSocket.close();
          				cluster.addFreeWorkerNode(n);
					synchronized(dos)
					{
						socket.failNum--;
					}
					socket.jobsscheduler.removeJob(myJob, myJob.numWorkers);
				}
				catch (Exception e) {
					System.out.println("///////////////////////////////////////failed worker");
					e.printStackTrace();
					fail = true;
				}
			}while(fail = true);
		}
	}

  	//the data structure for a cluster of worker nodes
  	class Cluster {
    		ArrayList<WorkerNode> workers; //all the workers
    		LinkedList<WorkerNode> freeWorkers; //the free workers
    
    		Cluster() {
      			workers = new ArrayList<WorkerNode>();
      			freeWorkers = new LinkedList<WorkerNode>();
    		}

    		WorkerNode createWorkerNode(String addr, int port) {
      			WorkerNode n = null;
      			synchronized(workers) {
        			n = new WorkerNode(workers.size(), addr, port);
        			workers.add(n);
      			}
      			addFreeWorkerNode(n);
			return n;
    		}

		int numTotalWorkers() {
			int n;
			synchronized(workers) {
				n = workers.size();
			}
			return n;
		}

    		WorkerNode getFreeWorkerNode() {
      			WorkerNode n = null;
      			try{
        			synchronized(freeWorkers) {
          				while(freeWorkers.size() == 0) {
            					freeWorkers.wait();
          				}
          			n = freeWorkers.remove();
        		}
        		n.status = 2;
      			} catch(Exception e) {
        			e.printStackTrace();
      			}
			return n;
    		}

    		void addFreeWorkerNode(WorkerNode n) {
      			n.status = 1;
      			synchronized(freeWorkers) {
        			freeWorkers.add(n);
        			freeWorkers.notifyAll();
      			}
    		}

    		int numFreeWorkerNode() {
			int size;
			synchronized(freeWorkers) {
        			size = freeWorkers.size();
      			}
			return size;
    		}
  	}

  	//the data structure of a worker node
  	class WorkerNode {
    		int id;
    		String addr;
    		int port;
    		int status; //WorkerNode status: 0-sleep, 1-free, 2-busy, 4-failed

    		WorkerNode(int i, String a, int p) {
      			id = i;
      			addr = a;
      			port = p;
      			status = 0;
    		}
  	}

	class Job {
		int jobID;
		String className;
		int numTasks;
		boolean scheduled;
		int numWorkers;
		boolean done_scheduling;
		int taskIdStart;
		boolean failedJob;
		boolean killedJob;
		boolean kill;
		
		Job(int j, String c, int n, int s, boolean f, boolean k) {
			jobID = j;
			className = c;
			numTasks = n;
			taskIdStart = s;
			failedJob = f;
			killedJob = k;
			scheduled = false;
			numWorkers = 0;
			done_scheduling=false;
			kill = false;
		}
		
	}
}
