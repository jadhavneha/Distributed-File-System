package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

var NODE_ID = ""
var LOCAL_IP = ""
var RING_POSITION = -1
var nodeLogger *Logger
var IsLeaderNode bool

type Node struct {
	ID           string
	LocalIP      string
	RingPosition int
	IsLeader     bool
	Logger       *Logger
}

func main() {
	wipeLocalHyDFSStorageOnBoot()

	node := &Node{
		RingPosition: -1,
	}

	clientServerChan := make(chan int, 5)

	ip, err := GetLocalIP()
	if err != nil {
		log.Fatalf("Unable to get local IP: %v", err)
	}
	node.LocalIP = ip
	LOCAL_IP = ip
	fmt.Println("DEBUG: Local IP detected by code:", node.LocalIP)

	nodeLogger = NewLogger(node.LocalIP)
	node.Logger = nodeLogger

	if node.LocalIP == LEADER_SERVER_HOST {
		node.IsLeader = true
	}
	IsLeaderNode = node.IsLeader
	fmt.Println("Is leader?:", node.IsLeader)
	fmt.Println("Configured leader host:", LEADER_SERVER_HOST)

	go startServer(node.Logger, clientServerChan, node.IsLeader)

	if err := addNodeToRing(node.IsLeader, LEADER_SERVER_HOST, node.Logger); err != nil {
		fmt.Println("addNodeToRing failed:", err)
	}

	clientServerChan <- 1

	go startClient(clientServerChan)

	reader := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !reader.Scan() {
			break
		}

		input := strings.TrimSpace(reader.Text())
		if input == "" {
			continue
		}

		args := strings.Fields(input)
		command := strings.ToLower(args[0])

		commandHandlers := map[string]func([]string){
			"list_mem": func(args []string) {
				members := GetSortedRingMembers()
				if len(members) == 0 {
					fmt.Println("No members known yet.")
					return
				}
				fmt.Println("Ring membership (sorted by ring position):")
				for i, m := range members {
					self := ""
					if m.Id == NODE_ID {
						self = "  <- self"
					}
					fmt.Printf("%2d) %s  [ring=%d]%s\n", i, m.Id, m.RingPosition, self)
				}
			},

			"list_self": func(args []string) {
				fmt.Printf("ID: %s POINT: %d\n", NODE_ID, RING_POSITION)
			},
			"piggybacks": func(args []string) { PrintPiggybackMessages() },
			"leave":      func(args []string) { ExitGroup() },
			"meta_info":  func(args []string) { fmt.Printf("ID: %s\n", node.ID) },

			"create": func(args []string) {
				if len(args) < 3 {
					fmt.Println("Usage: create <src> <dest>")
					return
				}
				err := CreateFileOnHDFS(args[1], args[2])
				if err != nil {
					if strings.Contains(err.Error(), "already existed") {
						fmt.Println(err.Error())
					} else {
						fmt.Println("Error while creating file:", err)
					}
				} else {
					fmt.Println("Creation completed")
				}
			},

			"print_succ": func(args []string) {
				fmt.Print(GetSuccessorNodes(RING_POSITION))
			},

			"list_ring": func(args []string) { PrintRingState() },

			"append": func(args []string) {
				if len(args) < 3 {
					fmt.Println("Usage: append <localfile> <HyDFSfilename>")
					return
				}
				if err := AppendLocalFileToHDFS(args[1], args[2]); err != nil {
					fmt.Println("Error appending:", err)
				} else {
					fmt.Println("Append completed")
				}
			},

			"multiappend": func(args []string) {
				if len(args) < 4 || (len(args)-2)%2 != 0 {
					fmt.Println("Usage: multiappend <HyDFSfilename> <VMi> <localfilenamei> ... <VMj> <localfilenamej>")
					return
				}
				var wg sync.WaitGroup

				hdfsFile := args[1]
				numPairs := (len(args) - 2) / 2
				errChan := make(chan error, numPairs)
				fmt.Println("Client: Starting CONCURRENT multi-append...")
				for i := 0; i < numPairs; i++ {
					wg.Add(1)
					vmIP := args[2+i*2]
					localFile := args[2+i*2+1]

					go func(vm string, file string) {
						defer wg.Done()

						fmt.Printf("Client: [LAUNCH] Sending append from VM '%s' for HDFS file '%s'\n", vm, hdfsFile)

						err := AppendLocalFileToHDFSOnNode(file, hdfsFile, vm)

						if err != nil {
							fmt.Printf("Client: [ERROR] Append from %s: %v\n", vm, err)
							errChan <- err
						} else {
							fmt.Printf("Client: [DONE] Append from %s finished.\n", vm)
						}
					}(vmIP, localFile)
				}

				fmt.Println("Client: Waiting for all appends to complete...")
				wg.Wait()
				close(errChan)

				if len(errChan) > 0 {
					fmt.Println("Client: Multi-append command completed with errors.")
					for err := range errChan {
						fmt.Printf("  - Error: %v\n", err)
					}
				} else {
					fmt.Println("Client: Multi-append command completed successfully.")
				}
			},

			"get": func(args []string) {
				if len(args) < 3 {
					fmt.Println("Usage: get <HyDFS_src> <local_dest>")
					return
				}
				hdfsFile := args[1]
				localFile := args[2]

				fmt.Printf("Client: Attempting to get file %s from local node %s\n", hdfsFile, NODE_ID)
				if err := DownloadHDFSFileToLocal(hdfsFile, localFile, NODE_ID); err == nil {
					fmt.Printf("Client: Successfully downloaded %s from local node %s\n", hdfsFile, NODE_ID)
					return
				} else {
					fmt.Printf("Client: File %s not found locally or error during local access: %v\n", hdfsFile, err)
				}

				members := GetAllMembers()
				for nodeID := range members {
					if nodeID == NODE_ID {
						continue
					}
					fmt.Printf("Client: Checking if node %s has file %s\n", nodeID, hdfsFile)
					if has, _ := remoteHasFileByNodeID(nodeID, hdfsFile); has {
						fmt.Printf("Client: Node %s has file %s. Attempting download...\n", nodeID, hdfsFile)
						if err := DownloadHDFSFileToLocal(hdfsFile, localFile, nodeID); err == nil {
							fmt.Printf("Client: Successfully downloaded %s from %s\n", hdfsFile, nodeID)
							return
						} else {
							fmt.Printf("Client: Error downloading %s from %s: %v\n", hdfsFile, nodeID, err)
						}
					}
				}

				fmt.Printf("Client: Could not find file %s on any of the VMs\n", hdfsFile)
			},

			"getfromreplica": func(args []string) {
				if len(args) < 4 {
					fmt.Println("Usage: getfromreplica <VMaddress> <HyDFS_src> <local_dest>")
					return
				}
				vmAddress := args[1]
				hdfsFile := args[2]
				localFile := args[3]

				nodeID, ok := GetNodeIDFromIP(vmAddress)
				if !ok {
					fmt.Printf("Error: VM address %s not found in membership list.\n", vmAddress)
					return
				}
				if err := DownloadHDFSFileToLocal(hdfsFile, localFile, nodeID); err != nil {
					fmt.Println("Error saving file:", err)
					return
				}
				fmt.Printf("Client: getfromreplica completed for %s from %s -> %s\n", hdfsFile, vmAddress, localFile)
			},

			"liststore": func(args []string) { ListLocalFiles() },

			"merge": func(args []string) {
				if len(args) < 2 {
					fmt.Println("Usage: merge <HyDFSfilename>")
					return
				}

				err := MergeHDFSFileOptimized(args[1])

				if err != nil {
					fmt.Println("Merge error:", err)
				} else {
					fmt.Println("Merge command sent.")
				}
			},

			"ls": func(args []string) {
				if len(args) < 2 {
					fmt.Println("Usage: ls <HyDFSfilename>")
					return
				}
				ShowFileLocations(args[1])
			},
			"rainstorm": func(args []string) {

				fmt.Println("DEBUG ARGS:", args)
				if len(args) < 11 {
					fmt.Println("Usage: rainstorm <Nstages> <Ntasks_per_stage> <op1> ... <opN> <hydfs_src_file> <hydfs_dest_file> <exactly_once> <autoscale> <input_rate> <LW> <HW>")
					return
				}

				nStages, err1 := strconv.Atoi(args[1])
				nTasks, err2 := strconv.Atoi(args[2])
				if err1 != nil || err2 != nil {
					fmt.Println("rainstorm: Nstages and Ntasks_per_stage must be integers")
					return
				}

				if len(args) < 2+nStages+7 {
					fmt.Printf("rainstorm: expected %d op specs, got %d\n", nStages, len(args)-3)
					fmt.Println("Usage: rainstorm <Nstages> <Ntasks_per_stage> <op1> ... <opN> <hydfs_src_file> <hydfs_dest_file> <exactly_once> <autoscale> <input_rate> <LW> <HW>")
					return
				}

				ops := make([]string, nStages)
				for i := 0; i < nStages; i++ {
					ops[i] = args[3+i]
				}

				base := 3 + nStages
				hydfsSrc := args[base]
				hydfsDest := args[base+1]
				exactlyOnce := (args[base+2] == "true")
				autoscale := (args[base+3] == "true")

				inputRate, err3 := strconv.Atoi(args[base+4])
				lw, err4 := strconv.Atoi(args[base+5])
				hw, err5 := strconv.Atoi(args[base+6])

				if err3 != nil || err4 != nil || err5 != nil {
					fmt.Println("rainstorm: input_rate, LW, HW must be integers")
					return
				}

				if err := StartRainStormJob(
					nStages,
					nTasks,
					ops,
					hydfsSrc,
					hydfsDest,
					exactlyOnce,
					autoscale,
					inputRate,
					lw,
					hw,
				); err != nil {
					fmt.Println("RainStorm start failed:", err)
				} else {
					fmt.Println("RainStorm job started.")
				}
			},
			"list_tasks": func(args []string) {
				if !IsLeaderNode {
					fmt.Printf("list_tasks: this node (%s) is not the leader. Please run on leader %s\n", LOCAL_IP, LEADER_SERVER_HOST)
					return
				}

				taskRegistry.mu.RLock()
				defer taskRegistry.mu.RUnlock()

				if len(taskRegistry.tasks) == 0 {
					fmt.Println("No active RainStorm tasks.")
					return
				}

				fmt.Println("Active RainStorm tasks:")
				for tid, info := range taskRegistry.tasks {
					vmID := info.nodeID
					vmIP := vmID
					if parts := strings.Split(vmID, "@"); len(parts) > 1 {
						vmIP = parts[0]
					}

					logFile := taskLogLocalPath(tid.JobID, tid.Stage, tid.Index)

					fmt.Printf("  %s => VM=%s PID=%d op=%s log=%s\n",
						tid.String(), vmIP, os.Getpid(), info.payload.OpExe, logFile)
				}
			},
			"kill_task": func(args []string) {
				if len(args) < 3 {
					fmt.Println("Usage: kill_task <stage> <index> [jobID]")
					return
				}

				stage, err1 := strconv.Atoi(args[1])
				index, err2 := strconv.Atoi(args[2])
				if err1 != nil || err2 != nil {
					fmt.Println("kill_task: stage and index must be integers")
					return
				}

				var jobID string
				if len(args) >= 4 {
					jobID = args[3]
				} else {
					jobID = GetLatestRainstormJobID()
					if jobID == "" {
						fmt.Println("kill_task: no RainStorm jobs found")
						return
					}
				}

				if err := KillTaskByStageIndex(jobID, stage, index); err != nil {
					fmt.Println("kill_task error:", err)
				} else {
					fmt.Printf("kill_task: requested kill of job=%s stage=%d index=%d\n", jobID, stage, index)
				}
			},

			"word-count": func(args []string) {
				if len(args) < 4 {
					fmt.Println("Usage: word-count <tasks_per_stage> <hydfs_src> <hydfs_dest>")
					return
				}
				tasksPerStage, err := strconv.Atoi(args[1])
				if err != nil {
					fmt.Println("word-count: tasks_per_stage must be an integer")
					return
				}
				hydfsSrc := args[2]
				hydfsDest := args[3]

				ops := []string{
					"tokenize",
					"aggregate:count",
				}

				if err := StartRainStormJob(
					2,
					tasksPerStage,
					ops,
					hydfsSrc,
					hydfsDest,
					true,
					false,
					1000,
					0,
					0,
				); err != nil {
					fmt.Println("Word-count job failed to start:", err)
				} else {
					fmt.Println("Word-count job started.")
				}
			},
		}

		if handler, ok := commandHandlers[command]; ok {
			handler(args)
		} else {
			fmt.Println("Unknown command:", command)
		}
	}
}

func wipeLocalHyDFSStorageOnBoot() {
	_ = os.RemoveAll(STORAGE_LOCATION)
	_ = os.MkdirAll(STORAGE_LOCATION, 0o755)

	fileInfoMap = map[string]*FileInfo{}
	fileBlockMap = map[string][]*FileBlock{}
	tempFileInfoMap = map[string]*FileInfo{}
	tempFileBlockMap = map[string][]*FileBlock{}
}
