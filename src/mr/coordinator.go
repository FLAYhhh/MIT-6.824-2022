package mr

import "time"
//import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type Coordinator struct {
	// Your definitions here.
	input_files []string
	nReduce int

	map_alloc_mu sync.Mutex
	map_alloc []bool

	reduce_alloc_mu sync.Mutex
	reduce_alloc []bool

	task_done_mu sync.Mutex
	map_done []bool // In this design, task is identified by file name
	reduce_done []bool  // reducer is identified by index

	start_reduce_mu sync.Mutex
	start_reduce bool
	start_reduce_cond *sync.Cond

	// all jobs are finshed
	finish_mu sync.Mutex
	finish_cond *sync.Cond
	finish bool
}

// Your code here -- RPC handlers for the worker to call.

// RPC handler: GetTasks
// FIXME: fault tolerance
func (c *Coordinator) GetTasks(args *GetTasksArgs, reply *GetTasksReply) error {
	c.finish_mu.Lock()
	defer c.finish_mu.Unlock()
	if c.finish {
		reply.Task_type = SHUTDOWN_TYPE
		return nil
	}

	c.start_reduce_mu.Lock()
	defer c.start_reduce_mu.Unlock()

	reply.NReduce = c.nReduce
	reply.NMap = len(c.input_files)
	reply.Task_type = WAIT_TYPE
	if !c.start_reduce {  // alloc Map task
		//fmt.Println("Have unfinished Map task")
		c.map_alloc_mu.Lock()
		defer c.map_alloc_mu.Unlock()

		for i, alloc := range c.map_alloc {
			if !alloc {
				reply.Task_type = MAP_TYPE
				reply.Task_id = i
				reply.File_name = c.input_files[i]
				c.map_alloc[i] = true
				// Fault Tolerance: if 10s later, c.map_done[i] is still false, make c.map_alloc[i] false
				go func(c *Coordinator, i int) {
					time.Sleep(10 * time.Second)

					c.task_done_mu.Lock()
					defer c.task_done_mu.Unlock()
					if c.map_done[i] == false {
						c.map_alloc_mu.Lock()
						defer c.map_alloc_mu.Unlock()
						c.map_alloc[i] = false
					}
				}(c, i)

				//fmt.Printf("Alloc Map task: ID = %v, File_name = %v\n", i, reply.File_name)
				break
			}
		}
		// TODO: if map tasks are allocated
	} else {  // alloc Reduce task
		// fmt.Println("Have unfinished Recuduce task")
		c.reduce_alloc_mu.Lock()
		defer c.reduce_alloc_mu.Unlock()

		for i, alloc := range c.reduce_alloc {
			if !alloc {
				reply.Task_type = REDUCE_TYPE
				reply.Task_id = i
				c.reduce_alloc[i] = true
				go func(c *Coordinator, i int) {
					time.Sleep(10 * time.Second)

					c.task_done_mu.Lock()
					defer c.task_done_mu.Unlock()
					if c.reduce_done[i] == false {
						c.reduce_alloc_mu.Lock()
						defer c.reduce_alloc_mu.Unlock()
						c.reduce_alloc[i] = false
					}
				}(c, i)
				//fmt.Printf("Alloc Reduce task: ID = %v\n", i);
				break
			}
		}
	}

	return nil
}

// RPC handler: TaskDone
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	Task_type := args.Task_type
	id := args.Id

	c.task_done_mu.Lock()
	defer c.task_done_mu.Unlock()
	if Task_type == MAP_TYPE {
		// check map_done[] and possibly set shart_reduce
		c.map_done[id] = true

		all_done := true
		for _, done := range c.map_done {
			if !done {
				all_done = false
			}
		}
		if all_done {
			c.start_reduce_mu.Lock()
			defer c.start_reduce_mu.Unlock()
			c.start_reduce = true;
		}
	} else if Task_type == REDUCE_TYPE {
		// check reduce_done[] and possibly set finish
		c.reduce_done[id] = true

		all_done := true
		for _, done := range c.reduce_done {
			if !done {
				all_done = false
			}
		}
		if all_done {
			c.finish_cond.Signal()
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
	c.finish_mu.Lock()
	c.finish_cond.Wait()
	c.finish = true
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

	in_len := len(files)
	c.input_files = files

	c.map_alloc = make([]bool, in_len)
	c.map_done = make([]bool, in_len)

	c.reduce_alloc = make([]bool, nReduce)
	c.reduce_done = make([]bool, nReduce)

	c.nReduce = nReduce
	c.start_reduce_cond = sync.NewCond(&c.start_reduce_mu)

	c.finish_cond = sync.NewCond(&c.finish_mu)

	c.server()
	return &c
}
