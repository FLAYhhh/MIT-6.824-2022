package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	input_files []string,
	nReduce int,

	map_alloc_mu sync.Mutex
	map_alloc map[int]bool

	reduce_alloc_mu sync.Mutex
	reduce_alloc map[int]bool

	task_done_mu sync.Mutex
	map_done map[int]bool, // In this design, task is identified by file name
	reduce_done map[int]bool,  // reducer is identified by index

	start_reduce_mu sync.Mutex
	start_reduce bool
	start_reduce_cond &sync.Cond

	// all jobs are finshed
	finish_mu sync.Mutex
	finish_cond &sync.Cond
	finish bool
}

// Your code here -- RPC handlers for the worker to call.

// RPC handler: GetTasks
// FIXME: fault tolerance
func (c *Coordinator) GetTasks(args *GetTasksArgs, reply *GetTasksReply) error {
	c.done_mu.lock()
	defer c.done_mu.unlock()
	if done {
		reply.task_type = SHUTDOWN_TYPE
		return nil
	}

	c.start_reduce_mu.lock()
	defer c.start_reduce_mu.unlock()

	reply.nReduce = c.nReduce
	reply.nMap = len(c.input_files)
	reply.task_type = WAIT_TYPE
	if !c.start_reduce {  // alloc Map task
		c.map_alloc_mu.lock()
		defer c.map_alloc_mu.unlock()

		for i, alloc : range c.map_alloc {
			if !alloc {
				reply.task_type = MAP_TYPE
				reply.task_id = i
				reply.file_name = input_files[i]
			}
		}
		// TODO: if map tasks are allocated
	} else {  // alloc Reduce task
		c.reduce_alloc_mu.lock()
		defer c.reduce_alloc_mu.lock()

		for i, alloc : range c.reduce_alloc {
			if !alloc {
				reply.task_type = REDUCE_TYPE
				reply.task_id = i
			}
		}
	}

	return nil
}

// RPC handler: TaskDone
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	task_type := args.task_type
	id := args.task_id

	task_done_mu.lock()
	defer task_done_mu.unlock()
	if task_type == MAP_TYPE {
		// check map_done[] and possibly set shart_reduce
		c.map_done[id] = true

		all_done := true
		for _, done : c.map_done {
			if !done {
				all_done = false
			}
		}
		if all_done {
			c.start_reduce_mu.lock()
			defer c.start_reduce_mu.unlock()
			c.start_reduce = true;
		}
	} else if task_type == REDUCE_TYPE {
		// check reduce_done[] and possibly set finish
		c.reduce_done[id] = true

		all_done := true
		for _, done : c.reduce_done {
			if !done {
				all_done = false
			}
		}
		if all_done {
			c.finish.signal()
		}
	} // else do nothing

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.finish_cond.wait()
	ret = true

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// TODO:  version #1 (do not support worker crash)
	// 1. generator map worker for each file, wait each worker to finish.
	// 2. when all map workers finish, start generate nReduce <reduce> workers.
	// 3. wait all <reduce> workers to finish
	// *4. How to do fault tolerance?
	//     Sol: if a worker consumes more than 10s to execute, start another worker to do the same thing.
	//
	//     But how to measure the time?
	//     Sol: set a Done flag with each work, using a thread to check whether it's finished after 10s.

	in_len = len(files)
	c.input_files = files

	c.map_alloc = make(map[int]bool, in_len)
	c.map_done = make(map[int]bool, in_len)

	c.reduce_alloc = make(map[int]bool, nReduce)
	c.reducer_done = make(map[int]bool, nReduce)

	c.nReduce = nReduce
	c.start_reduce_cond = sync.NewCond(c.start_reduce_mu)

	c.finish_cond = sync.NewCond(c.finish_mu)

	c.server()
	return &c
}
