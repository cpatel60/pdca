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

//////////////////////////////////////////////////////////////////////////////////////
  public void startServer() {
	try{
      		//create a ServerSocket listening at specified port
      		ServerSocket serverSocket = new ServerSocket(schedulerPort);
    		System.out.println( "scheduler socket started\n");
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

/*
class AssignWorker implements Runnable {
	public void run {
	
	}	
}
*/
class JobsScheduler implements Runnable {
	LinkedList<Job> JobsToSchedule;
	//int schedule_workers;
	int total_workers;
	int num_workers;

	public JobsScheduler() {
		//System.out.println("in jobsscheduler constructor");
		JobsToSchedule = new LinkedList<Job>();
		//schedule_workers = 0;
		total_workers = cluster.numTotalWorkers();
		num_workers = 0;
	}

	void addJob(Job job)
	{
		synchronized(JobsToSchedule) {
			if(job.failedJob == true)
			{
				JobsToSchedule.addFirst(job); //delete job??
				System.out.println("Added failed job: "+job.jobID);
			}	
			else
			{	
				JobsToSchedule.add(job); //delete job??
				System.out.println("Added job: "+job.jobID);
			}
		}
	}


	void removeJob(Job job, int n) {
		synchronized(JobsToSchedule) {
			System.out.println("Removing job: "+job.jobID);
			JobsToSchedule.remove(job);
			num_workers += n;
		}
	}
	
	public void run() {
		int time = 500;
		//schedule_workers = 
		//num_workers = 
		//total_workers = 
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
			//		System.out.println("total_workers: "+total_workers);
			//		System.out.println("num_workers: "+num_workers);
				if(size != 0 && numWorkers !=0) {
					System.out.println("Scheduling jobs. jobs to schedule = "+size+" workers available = "+numWorkers);
					int dmin = Integer.MAX_VALUE;
           				ListIterator<Job> itr = JobsToSchedule.listIterator();
					boolean all_scheduled = true; 
    					while( itr.hasNext() && numWorkers>0){
						Job job = itr.next();
						if(!job.scheduled)
						{
							all_scheduled = false;
							numWorkers--;
							job.scheduled = true;
							job.numWorkers = 1;
							if(job.numTasks<dmin && job.numTasks != 1 && job.failedJob == false)
							{
								dmin = job.numTasks;
							}
						}
					}
					System.out.println("All_scheduled: "+all_scheduled); 
					if(all_scheduled)
						time+=500;
					else
					{
						time=500;
						//while(numWorkers>0)
						{
							ListIterator<Job> itr2 = JobsToSchedule.listIterator();
    							while( itr2.hasNext() && numWorkers>0){
								int dsh = dmin;						
								dmin = Integer.MAX_VALUE;
								Job job = itr2.next();
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
								//System.out.println("here1");
							}
						}
						//System.out.println("here2");
						ListIterator<Job> itr1 = JobsToSchedule.listIterator();
    						while( itr1.hasNext()){
							Job job = itr1.next();
							if(job.scheduled)
							{
								job.done_scheduling=true;
								System.out.println("Done scheduling job "+job.jobID);
							}
						}
						//synchronized (num_workers) {
							num_workers = numWorkers;
						//}
					}
          			}
				else
					time+=500;
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

	/*{
		LinkedList<WorkerNode> RunningList;

		void addRunningList(RunningJob job)
		{
			synchronized(RunningList)
			{
				RunningList.add(job);
			}
		}
		void removeRunningList(RunningJob job)
		{
			synchronized(RunningList)
			{
				RunningList.remove(job);
			}
		}
		
		boolean lastRunningList()
		{
			boolean success;
			synchronized(RunningList)
			{
				try
				{
					RunningJob job = RunningList.removeLast();
					jobsscheduler.addJob();
					success = true;
				}
				catch(exception e)
				{
					success = false;
				}
			}
			return success;
		}
	}*/

	class RunningJob
	{
		int jobID;
		int jobIDStart;
		int numTasks;
		int worker;

		public RunningJob(int id, int start, int tasks, int w)
		{
			jobID = id;
			jobIDStart = start;
			numTasks = tasks;
			worker = w;
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

    		public ParallelSocket(Socket socket, int id, Scheduler server, Cluster cluster, JobsScheduler jobsscheduler) {
			this.socket = socket; //reqd??
			this.id = id;
			this.server = server; //reqd??
			this.jobsscheduler = jobsscheduler;
			failNum = 0;
			//System.out.println( "in parallelsocket" );
    		}

  		public void run() {
    			try{
				System.out.println( "in thread "+id);
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

	  				Job myJob = new Job(jobId, className, numTasks, 0, false);
	  				jobsscheduler.addJob(myJob);
	  				while(myJob.done_scheduling==false)
					{
						System.out.print("");
					}

	  				System.out.println("Job scheduled "+myJob.jobID);
					int numWorkers = myJob.numWorkers;
					//jobsscheduler.removeJob(myJob);
          				//notify the client
          				dos.writeInt(Opcode.job_start);
          				dos.flush();
					int i;
					int tasksAssigned = numTasks / numWorkers;
					int task_left=0, start_task=0;
					for(i=0; i<numWorkers-1; i++)
					{
						try {
						task_left = tasksAssigned;
						start_task = i*tasksAssigned;
						//get a free worker
          					WorkerNode n = cluster.getFreeWorkerNode();
					//	RunningJob job(jobId, i*tasksAssigned, tasksAssigned, n);
					//	addRunningList(job);
        					//System.out.println("get worker"+n+" thread"+id+ " taskid"+taskId);
          					//assign the tasks to the worker
          					Socket workerSocket = new Socket(n.addr, n.port);
          					DataInputStream wis = new DataInputStream(workerSocket.getInputStream());
          					DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());
          
          					wos.writeInt(Opcode.new_tasks);
          					wos.writeInt(jobId);
          					wos.writeUTF(className);
     						//     wos.writeInt(taskIdStart);
     						//     wos.writeInt(numTasks);
						wos.writeInt(i*tasksAssigned);
          					wos.writeInt(tasksAssigned);
          					wos.flush();

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
          					}

					//	removeRunningList(job);

          					//disconnect and free the worker
          					wis.close();
          					wos.close();
          					workerSocket.close();
          					cluster.addFreeWorkerNode(n);
						}
						catch (Exception e) {
							System.out.println("///////////////////////////////////////scheduler");
							e.printStackTrace();
							numWorkers--;
							Job failJob = new Job(jobId, className, task_left, start_task, true);
							FaultTolerance faultcase = new FaultTolerance(failJob, dos, this);
							new Thread(faultcase).start();
							synchronized(dos)
							{
								failNum++;
							}
						}
					}
					try {
					task_left = tasksAssigned + (numTasks % numWorkers);
					start_task = i*tasksAssigned;
					//get a free worker
          				WorkerNode n = cluster.getFreeWorkerNode();
          				//System.out.println("get worker"+n+" thread"+id+ " taskid"+taskId);
          				//assign the tasks to the worker
          				Socket workerSocket = new Socket(n.addr, n.port);
          				DataInputStream wis = new DataInputStream(workerSocket.getInputStream());
          				DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());
             				wos.writeInt(Opcode.new_tasks);
          				wos.writeInt(jobId);
          				wos.writeUTF(className);
     					//     wos.writeInt(taskIdStart);
     					//     wos.writeInt(numTasks);
					wos.writeInt(start_task);
          				wos.writeInt(task_left);
          				wos.flush();

					//repeatedly process the worker's feedback
          				while(wis.readInt() == Opcode.task_finish) {
						start_task++;
						task_left--;
						synchronized (dos)
						{
            					dos.writeInt(Opcode.job_print);
            					dos.writeUTF("task "+wis.readInt()+" finished on worker "+n.id);
            					dos.flush();
						}
          				}
					//disconnect and free the worker
          				wis.close();
          				wos.close();
          				workerSocket.close();
          				cluster.addFreeWorkerNode(n);

					while(failNum!=0);

					//notify the client
					synchronized(dos){
          				dos.writeInt(Opcode.job_finish);
          				dos.flush();
					}
					}
					catch (Exception e) {
						System.out.println("///////////////////////////////////////scheduler");
						e.printStackTrace();
						numWorkers--;
						Job failJob = new Job(jobId, className, task_left, start_task, true);
						FaultTolerance faultcase = new FaultTolerance(failJob, dos, this);
						new Thread(faultcase).start();
						synchronized(dos)
							{
								failNum++;
							}
					}
					//jobsscheduler.incNumWorkers(numWorkers);
					jobsscheduler.removeJob(myJob, numWorkers);
				}

        			dis.close();
        			dos.close();
        			socket.close();
    			} 
			catch(Exception e) {
      				e.printStackTrace();
    			}
      			//serverSocket.close();
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
			{
				System.out.print("");
			}

	  		System.out.println("Job scheduled "+myJob.jobID);
			int numWorkers = myJob.numWorkers;
			//jobsscheduler.removeJob(myJob);
          		//notify the client
          	//	dos.writeInt(Opcode.job_start);
          	//	dos.flush();
			try {
				//get a free worker
          			WorkerNode n = cluster.getFreeWorkerNode();
			//	RunningJob job(jobId, i*tasksAssigned, tasksAssigned, n);
			//	addRunningList(job);
        			//System.out.println("get worker"+n+" thread"+id+ " taskid"+taskId);
          			//assign the tasks to the worker
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
				//socket.write(wis.readInt(),n.id);
				System.out.println("/////////////////////////////////task "+wis.readInt()+" finished on worker "+n.id);
				/*
				synchronized(dos)
				{
            			dos.writeInt(Opcode.job_print);
            			dos.writeUTF("task "+wis.readInt()+" finished on worker "+n.id);
            			dos.flush();
				}*/
          			}

          			//disconnect and free the worker
          			wis.close();
          			wos.close();
          			workerSocket.close();
          			cluster.addFreeWorkerNode(n);

				socket.jobsscheduler.removeJob(myJob, myJob.numWorkers);
				synchronized(dos)
							{
								socket.failNum--;
							}
			}
			catch (Exception e) {
				System.out.println("///////////////////////////////////////failed worker");
				e.printStackTrace();
			//	numWorkers--;
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
	//ListIterator i=freeWorkers.listIterator(0);
	//System.out.println("LinkedList:" + freeWorkers);
	/*ListIterator itr = freeWorkers.listIterator();
    while( itr.hasNext() ){
        System.out.println( "list:"+itr.next() );
    }*/
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
		
		Job(int j, String c, int n, int s, boolean f) {
			jobID = j;
			className = c;
			numTasks = n;
			taskIdStart = s;
			failedJob = false;
			scheduled = false;
			numWorkers = 0;
			done_scheduling=false;
		}	
	}
}
