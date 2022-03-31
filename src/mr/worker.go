package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// 1. use RPC to get tasks
	for {
		task_info := CallGetTasks()
		switch task_info.task_type {
			case MAP_TYPE:
				do_map_task(mapf, task_info)
			case REDUCE_TYPE:
				do_reduce_task(reducef, task_info)
			case WAIT_TYPE:
				// TODO: sleep
			case SHUTDOWN_TYPE:
				break;
		}
		// DONE: use RPC to tell coordinator we're done
		CallTaskDone(task_info)
	}
}

func do_reduce_task(reducef func(string, []string) string,
	task_info GetTasksReply) {

	// DONE: read mr-<*>-<id>
	id = task_info.task_id
	nMap = task_info.nMap

	kva := []mr.KeyValue{}
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprint("mr-%d-%d", i, id)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprint("mr-out-%d", id)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-id.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

}

func do_map_task(mapf func(string, string) []KeyValue,
		task_info GetTasksReply) {

	nReduce = task_info.nReduce
	id = task_info.task_id
	//
	// read input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediates := make([][]mr.KeyValue, nReduce)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	for _, kv : range kva {
		which_reducer = ihash(kv.Key) % nReduce
		intermediates[which_reducer] = append(intermediates[which_reducer], kv)
	}

	for i := 0; i < nReduce; i++ {
		oname = fmt.Sprint("mr-%d-%d", id, i)
		// FIXME: use tmp file and rename
		ofile, _ := os.Create(oname)
		// TODO: write intermedaites[i] to mr-<map_id>-<i>
		enc := json.NewEncoder(ofile)
		for _, kv := intermediates[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("encode err! kv: %v\n", kv)
			}
		}
	}
}

func CallGetTasks() GetTasksReply {
	args := GetTasksArgs{}

	reply := GetTasksReply{}
	ok := call("Coordinator.GetTasks", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Get Task | type: %v", reply.task_type)
		if reply.task_type == MAP_TYPE {
			fmt.Printf(" | map ID: %v | input file: %v | nReduce: %v\n",
				reply.task_id, reply.file_name, reply.nReduce);
		} else if reply.task_type == REDUCE_TYPE {
			fmt.Printf(" | reduce ID: %v | nMap: %v\n", reply.task_id, reply.nMap);
		} else if reply.task_type == SHUTDOWN_TYPE {
			fmt.Printf("\nbye\n");
		} else if reply.task_type == WAIT_TYPE {
			fmt.Printf("\nwait...\n");
			// TODO: sleep
		}
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply
}

func CallTaskDone(task_info GetTasksReply) {
	args := TaskDoneArgs{}
	reply := TaskDoneReply{}

	args.id = task_info.task_id
	args.task_type = task_info.task_type

	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok {
		fmt.Printf("inform coordinator success\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
