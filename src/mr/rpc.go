package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
// TODO: define what worker needs to talk to coordinator

// need to get tasks
type GetTasksArgs struct {

}

const MAP_TYPE = "MAP"
const REDUCE_TYPE = "REDUCE"
const SHUTDOWN_TYPE = "SHUTDOWN"
const WAIT_TYPE = "WAIT"

type GetTasksReply struct {
	Task_type string // Must be consistent with RPC server, 2 types: MAP_TYPE and REDUCE_TYPE
	Task_id int       // (task_type, task_id) identifies a task
	NReduce int
	NMap    int      // WARNNING: this var can only be set when all map workers finished
                      // and only used by reduce task
                      // In this design, nMap can be simplified to Inpute file numbers

	File_name string  // only used by map worker
}

type TaskDoneArgs struct {
	Task_type string
	Id int
}

type TaskDoneReply struct {
	// empty
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
