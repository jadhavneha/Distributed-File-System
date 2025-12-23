package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	fileInfoMap      = map[string]*FileInfo{}
	fileBlockMap     = map[string][]*FileBlock{}
	tempFileInfoMap  = map[string]*FileInfo{}
	tempFileBlockMap = map[string][]*FileBlock{}

	blockSeenMap = map[string]map[int]bool{}
)
var fileStateMutex = sync.RWMutex{}

type FileInfo struct {
	Name       string
	NumBlocks  int
	IsPrimary  bool
	MostRecent bool
	Replicated bool
}

// FileBlock stores a block of a HyDFS file
type FileBlock struct {
	Name       string
	Content    []byte
	BlockID    int
	Replicated bool
}

// GetMessage is used to request a file from another node
type GetMessage struct {
	Name      string
	Requester string
}

type checkResp struct {
	Has        bool `json:"has"`
	BlockCount int  `json:"blocks"`
}

func decodeFileInfo(data any, out *FileInfo) error {
	switch v := data.(type) {
	case string:
		return json.Unmarshal([]byte(v), out)
	case []byte:
		return json.Unmarshal(v, out)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return json.Unmarshal(b, out)
	}
}

func decodeFileBlock(data any, out *FileBlock) error {
	switch v := data.(type) {
	case string:
		return json.Unmarshal([]byte(v), out)
	case []byte:
		return json.Unmarshal(v, out)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return json.Unmarshal(b, out)
	}
}

// UpdatePrimaryFiles marks files where this node is now the primary replica
func UpdatePrimaryFiles() []*FileInfo {
	fileStateMutex.RLock()
	defer fileStateMutex.RUnlock()

	var primaryFiles []*FileInfo
	for _, f := range fileInfoMap {
		if !f.IsPrimary && GetPrimaryReplicaNode(f.Name) == NODE_ID {
			f.IsPrimary = true
		}
		if f.IsPrimary {
			primaryFiles = append(primaryFiles, f)
		}
	}
	return primaryFiles
}

// ListLocalFiles prints all files stored on this node
func ListLocalFiles() {
	fmt.Printf("=== Local HyDFS Files (This VM ID: %s  RingID: %d) ===\n", NODE_ID, RING_POSITION)
	if len(fileInfoMap) == 0 {
		fmt.Println("  (no files)")
		fmt.Println("===============================================")
		return
	}

	keys := make([]string, 0, len(fileInfoMap))
	for k := range fileInfoMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		fi := fileInfoMap[k]

		blkCount := localBlockCount(fi.Name)
		if blkCount == 0 {
			continue
		}

		primaryMark := ""
		if fi.IsPrimary {
			primaryMark = "*"
		}
		display := fi.Name
		if display == "" {
			display = k
		}
		fileID := ComputeRingPosition(display)
		fmt.Printf("  %s %s  (fileID=%d, blocks=%d)\n",
			display, primaryMark, fileID, blkCount)
	}
	fmt.Println("===============================================")
}

func localBlockCount(name string) int {
	fileStateMutex.RLock()
	defer fileStateMutex.RUnlock()

	if blks, ok := fileBlockMap[name]; ok && len(blks) > 0 {
		return len(blks)
	}
	if blks, ok := tempFileBlockMap[name]; ok && len(blks) > 0 {
		return len(blks)
	}
	return 0
}

func remoteHasFile(host string, name string) (bool, int) {
	if strings.Contains(host, "@") {
		host = GetIPFromID(host)
	}
	if host == "" {
		return false, -2
	}

	addr := net.JoinHostPort(host, fmt.Sprint(SERVER_PORT))
	conn, err := net.DialTimeout("tcp", addr, 1500*time.Millisecond)
	if err != nil {
		return false, -3
	}
	defer conn.Close()

	req := NewHasFileRequest(name)
	bw := bufio.NewWriter(conn)
	b, err := req.Encode()
	if err != nil {
		return false, -4
	}
	if _, err := bw.Write(append(b, '\n')); err != nil {
		return false, -4
	}
	if err := bw.Flush(); err != nil {
		return false, -4
	}

	conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil || len(line) == 0 {
		return false, -5
	}

	var resp Message
	if err := json.Unmarshal([]byte(line), &resp); err != nil || resp.Kind != HAS_FILE_REPLY {
		return false, -6
	}

	var payload struct {
		Present bool `json:"present"`
		Code    int  `json:"code"`
	}
	switch data := resp.Data.(type) {
	case string:
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			rb, _ := json.Marshal(resp.Data)
			_ = json.Unmarshal(rb, &payload)
		}
	case map[string]any:
		if v, ok := data["present"].(bool); ok {
			payload.Present = v
		}
		if v, ok := data["code"].(float64); ok {
			payload.Code = int(v)
		}
	default:
		rb, _ := json.Marshal(resp.Data)
		_ = json.Unmarshal(rb, &payload)
	}

	return payload.Present, payload.Code
}

// ShowFileLocations prints all nodes that actually store the given file (have ≥1 blocks)
func ShowFileLocations(hdfsFile string) {
	filePos := ComputeRingPosition(hdfsFile)

	fmt.Printf("\nFile: %s (FileID: %d)\n", hdfsFile, filePos)

	hasOn := func(nodeID string) bool {
		if nodeID == NODE_ID {
			return localBlockCount(hdfsFile) > 0
		}
		ok, _ := remoteHasFileByNodeID(nodeID, hdfsFile)
		return ok
	}

	fmt.Println("Replicas discovered:")
	for nid := range membershipInfo {
		if hasOn(nid) {
			ip := GetIPFromID(nid)
			fmt.Printf("  VM: %-15s  NodeID: %s  RingID: %d  present=1\n",
				ip, nid, ComputeRingPosition(nid))
		}
	}
	fmt.Println()
}

func remoteHasFileByNodeID(nodeID, fname string) (bool, int) {
	ip := GetIPFromID(nodeID)
	if ip == "" {
		return false, -1
	}
	ok, code := remoteHasFile(ip, fname)
	return ok, code
}

// CreateFileOnHDFS creates a new HyDFS file from a local file
func CreateFileOnHDFS(localFile, hdfsFile string) error {
	primaryNode := GetPrimaryReplicaNode(hdfsFile)
	if primaryNode == "" {
		return fmt.Errorf("could not determine primary node for %s", hdfsFile)
	}

	var exists bool
	var err error

	if primaryNode == NODE_ID {
		fileStateMutex.Lock()
		_, exists = fileInfoMap[hdfsFile]
		if !exists {
			_, exists = tempFileInfoMap[hdfsFile]
		}
		fileStateMutex.Unlock()
	} else {
		member, ok := GetMember(primaryNode)
		if !ok {
			return fmt.Errorf("primary node %s not in membership list", primaryNode)
		}
		exists, _ = remoteHasFile(member.Host, hdfsFile)
	}

	if exists {
		return fmt.Errorf("file %s already existed", hdfsFile)
	}

	content, err := os.ReadFile(localFile)
	if err != nil {
		return err
	}

	fileInfo := FileInfo{Name: hdfsFile, NumBlocks: 0, IsPrimary: false, MostRecent: false, Replicated: false}
	createMsg := Message{Kind: CREATE, Data: string(MustMarshal(fileInfo))}

	block := FileBlock{Name: hdfsFile, Content: content, BlockID: -1, Replicated: false}
	appendMsg := Message{Kind: APPEND, Data: string(MustMarshal(block))}

	if primaryNode == NODE_ID {
		if err := ProcessCreateMessage(createMsg, false); err != nil {
			return err
		}
		return ProcessAppendMessage(appendMsg, false)
	}

	member, ok := GetMember(primaryNode)
	if !ok {
		return fmt.Errorf("Create: could not find primary member %s", primaryNode)
	}

	if err := SendMessage(member, createMsg); err != nil {
		return err
	}

	targets := DesiredReplicaSet(hdfsFile, NUM_REPLICAS)
	ips := make([]string, 0, len(targets))
	for _, t := range targets {
		if t == "" {
			continue
		}
		ips = append(ips, GetIPFromID(t))
	}
	return SendMessage(member, appendMsg)
}

// AppendLocalFileToHDFS appends a local file to an existing HDFS file
func AppendLocalFileToHDFS(localFile, hdfsFile string) error {
	content, err := os.ReadFile(localFile)
	if err != nil {
		return err
	}

	appendBlock := FileBlock{Name: hdfsFile, Content: content, BlockID: -1, Replicated: false}
	appendMsg := Message{Kind: APPEND, Data: string(MustMarshal(appendBlock))}
	primaryNode := GetPrimaryReplicaNode(hdfsFile)

	if primaryNode == NODE_ID {
		return ProcessAppendMessage(appendMsg, false)
	}

	member, ok := GetMember(primaryNode)
	if !ok {
		return fmt.Errorf("Append: could not find primary member %s", primaryNode)
	}

	targets := DesiredReplicaSet(hdfsFile, NUM_REPLICAS)
	ips := make([]string, 0, len(targets))
	for _, t := range targets {
		if t == "" {
			continue
		}
		ips = append(ips, GetIPFromID(t))
	}
	return SendMessage(member, appendMsg)
}

func RequestHDFSFile(hdfsFile, nodeID string) error {
	targetNode := nodeID
	if targetNode == "" {
		targetNode = GetPrimaryReplicaNode(hdfsFile)
	}

	member, ok := GetMember(targetNode)
	if !ok {
		return fmt.Errorf("Request: could not find target member %s", targetNode)
	}

	checkMsg := Message{Kind: CHECK, Data: map[string]string{"Name": hdfsFile}}
	respMsg, err := SendMessageGetReply(member, checkMsg)
	if err != nil {
		return err
	}

	if respMsg.Kind != ACK {
		return fmt.Errorf("expected ACK from CHECK, got %s", respMsg.Kind.String())
	}

	var info checkResp

	dataStr, ok := respMsg.Data.(string)
	if !ok {
		return fmt.Errorf("failed to cast ACK data to string, got %T", respMsg.Data)
	}
	if dataStr == "" {
		return fmt.Errorf("ACK data was empty string")
	}

	var innerMsg Message
	if err := json.Unmarshal([]byte(dataStr), &innerMsg); err != nil {
		return fmt.Errorf("failed to decode inner message from ACK data: %v", err)
	}

	dataBytes, err := json.Marshal(innerMsg.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal inner message data: %v", err)
	}
	if err := json.Unmarshal(dataBytes, &info); err != nil {
		return fmt.Errorf("failed to decode check response from inner message: %v", err)
	}

	if !info.Has {
		return fmt.Errorf("file %s does not exist on node %s", hdfsFile, targetNode)
	}

	getMsg := GetMessage{Name: hdfsFile, Requester: NODE_ID}
	return SendMessage(member, Message{
		Kind: GETFILE,
		Data: string(MustMarshal(getMsg)),
	})
}

func DownloadHDFSFileForRainStorm(hdfsFile, localFile, nodeID string) error {

	writeBlocks := func(blocks []*FileBlock) error {
		sort.Slice(blocks, func(i, j int) bool { return blocks[i].BlockID < blocks[j].BlockID })
		f, err := os.Create(localFile)
		if err != nil {
			return err
		}
		defer f.Close()
		for _, b := range blocks {
			if _, err := f.Write(b.Content); err != nil {
				return err
			}
		}
		return f.Sync()
	}

	if nodeID != "" || GetPrimaryReplicaNode(hdfsFile) != NODE_ID {
		if err := RequestHDFSFile(hdfsFile, nodeID); err != nil {
			if bs := fileBlockMap[hdfsFile]; len(bs) > 0 {
				return writeBlocks(bs)
			}
			return err
		}

		deadline := time.Now().Add(15 * time.Second)
		for {
			fileStateMutex.RLock()
			info, infoOK := tempFileInfoMap[hdfsFile]
			blocks, blocksOK := tempFileBlockMap[hdfsFile]
			fileStateMutex.RUnlock()

			if infoOK && blocksOK && info.NumBlocks > 0 && info.NumBlocks == len(blocks) {
				if err := writeBlocks(blocks); err != nil {
					return err
				}
				fileStateMutex.Lock()
				delete(tempFileBlockMap, hdfsFile)
				delete(tempFileInfoMap, hdfsFile)
				fileStateMutex.Unlock()
				fmt.Println("Download completed")
				return nil
			}
			if time.Now().After(deadline) {
				fileStateMutex.RLock()
				bs, ok := fileBlockMap[hdfsFile]
				fileStateMutex.RUnlock()
				if ok && len(bs) > 0 {
					return writeBlocks(bs)
				}
				return fmt.Errorf("timed out fetching %q from replica (got %d blocks, expected %d)", hdfsFile, len(blocks), info.NumBlocks)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	if bs := fileBlockMap[hdfsFile]; len(bs) > 0 {
		if err := writeBlocks(bs); err != nil {
			return err
		}
		fmt.Println("Download completed")
		return nil
	}
	return fmt.Errorf("no blocks found locally for %q", hdfsFile)
}

func DownloadHDFSFileToLocal(hdfsFile, localFile, nodeID string) error {
	// Helper to write blocks in BlockID order
	writeBlocks := func(blocks []*FileBlock) error {
		sort.Slice(blocks, func(i, j int) bool { return blocks[i].BlockID < blocks[j].BlockID })
		f, err := os.Create(localFile)
		if err != nil {
			return err
		}
		defer f.Close()
		for _, b := range blocks {
			if _, err := f.Write(b.Content); err != nil {
				return err
			}
		}
		return f.Sync()
	}

	// If caller specified a replica or the primary is remote, request blocks from the remote
	if nodeID != "" || GetPrimaryReplicaNode(hdfsFile) != NODE_ID {
		if err := RequestHDFSFile(hdfsFile, nodeID); err != nil {
			if bs := fileBlockMap[hdfsFile]; len(bs) > 0 {
				return writeBlocks(bs)
			}
			return err
		}

		deadline := time.Now().Add(100 * time.Second)
		for {
			fileStateMutex.RLock()
			info, infoOK := tempFileInfoMap[hdfsFile]
			blocks, blocksOK := tempFileBlockMap[hdfsFile]
			fileStateMutex.RUnlock()

			if infoOK && blocksOK && info.NumBlocks > 0 && info.NumBlocks == len(blocks) {
				if err := writeBlocks(blocks); err != nil {
					return err
				}
				fileStateMutex.Lock()
				delete(tempFileBlockMap, hdfsFile)
				delete(tempFileInfoMap, hdfsFile)
				fileStateMutex.Unlock()
				fmt.Println("Download completed")
				return nil
			}
			if time.Now().After(deadline) {
				fileStateMutex.RLock()
				bs, ok := fileBlockMap[hdfsFile]
				fileStateMutex.RUnlock()
				if ok && len(bs) > 0 {
					return writeBlocks(bs)
				}
				return fmt.Errorf("timed out fetching %q from replica (got %d blocks, expected %d)", hdfsFile, len(blocks), info.NumBlocks)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	if bs := fileBlockMap[hdfsFile]; len(bs) > 0 {
		if err := writeBlocks(bs); err != nil {
			return err
		}
		fmt.Println("Download completed")
		return nil
	}
	return fmt.Errorf("no blocks found locally for %q", hdfsFile)
}

// MergeHDFSFile merges and replicates file blocks to successors
func MergeHDFSFileOptimized(hdfsFile string) error {
	if GetPrimaryReplicaNode(hdfsFile) != NODE_ID {
		primaryNode := GetPrimaryReplicaNode(hdfsFile)
		member, ok := GetMember(primaryNode)
		if !ok {
			return fmt.Errorf("Merge: could not find primary member %s", primaryNode)
		}
		return SendMessage(member, Message{Kind: MERGE, Data: hdfsFile})
	}

	startTime := time.Now()

	fileStateMutex.Lock()
	info, ok := fileInfoMap[hdfsFile]
	if !ok {
		fileStateMutex.Unlock()
		return nil
	}

	if len(fileBlockMap[hdfsFile]) == 0 {
		fileStateMutex.Unlock()
		return nil
	}

	if info.MostRecent {
		fmt.Printf("File %s has no new updates. Merge skipped.\n", hdfsFile)
		fileStateMutex.Unlock()

		duration := time.Since(startTime)
		nodeLogger.Log("MEASURE", "Merge command finished for %s. Duration: %v", hdfsFile, duration)
		return nil
	}

	blocksToReplicate := make([]*FileBlock, len(fileBlockMap[hdfsFile]))
	copy(blocksToReplicate, fileBlockMap[hdfsFile])
	fileStateMutex.Unlock()

	for _, succ := range GetSuccessorNodes(RING_POSITION) {
		member, ok := GetMember(succ)
		if !ok {
			fmt.Printf("Merge: could not find member %s to send DELETE\n", succ)
			continue
		}
		SendMessage(member, Message{Kind: DELETE, Data: hdfsFile})
	}

	replicateFileInfo := FileInfo{Name: hdfsFile, NumBlocks: 0, IsPrimary: false, MostRecent: true, Replicated: true}
	if err := PerformReplication(Message{Kind: CREATE, Data: string(MustMarshal(replicateFileInfo))}, true); err != nil {
		return fmt.Errorf("merge: CREATE replicate failed: %w", err)
	}

	for _, block := range blocksToReplicate {
		repBlock := *block
		repBlock.Replicated = true
		if err := PerformReplication(Message{Kind: APPEND, Data: string(MustMarshal(repBlock))}, true); err != nil {
			return fmt.Errorf("merge: APPEND replicate failed (block %d): %w", repBlock.BlockID, err)
		}
	}

	fileStateMutex.Lock()
	info, ok = fileInfoMap[hdfsFile]
	if ok {
		info.MostRecent = true
	}
	fileStateMutex.Unlock()

	duration := time.Since(startTime)
	nodeLogger.Log("MEASURE", "Merge command finished for %s. Duration: %v", hdfsFile, duration)

	return nil
}

func uniqueStrings(in []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func PerformReplication(message Message, waitAll bool) error {
	fname := func() string {
		switch v := message.Data.(type) {
		case string:
			var fi FileInfo
			if json.Unmarshal([]byte(v), &fi) == nil && fi.Name != "" {
				return fi.Name
			}
			var blk FileBlock
			if json.Unmarshal([]byte(v), &blk) == nil && blk.Name != "" {
				return blk.Name
			}
		case map[string]any:
			b, _ := json.Marshal(v)
			var fi FileInfo
			if json.Unmarshal(b, &fi) == nil && fi.Name != "" {
				return fi.Name
			}
			var blk FileBlock
			if json.Unmarshal(b, &blk) == nil && blk.Name != "" {
				return blk.Name
			}
		}
		return ""
	}()
	if fname == "" {
		return fmt.Errorf("replication: could not determine file name")
	}

	targets := DesiredReplicaSet(fname, NUM_REPLICAS)
	successors := []string{}
	for _, t := range targets {
		if t != "" && t != NODE_ID {
			successors = append(successors, t)
		}
	}
	successors = uniqueStrings(successors)
	fmt.Printf("[NODE %s] Replicating %s for %s to successors: %v\n", NODE_ID, message.Kind.String(), fname, successors)
	ch := make(chan error, len(successors))

	for _, succ := range successors {
		member, ok := GetMember(succ)
		if !ok {
			fmt.Printf("[NODE %s] Replicating: could not find member %s\n", NODE_ID, succ)
			go func(s string) { ch <- fmt.Errorf("could not find member %s", s) }(succ)
			continue
		}
		go SendAnyReplicationMessage(member, message, ch)
	}

	var err error
	if waitAll {
		timeout := time.After(2 * time.Second)
		for i := 0; i < len(successors); i++ {
			select {
			case e := <-ch:
				if e != nil {
					err = e
				}
			case <-timeout:
				return fmt.Errorf("replication timed out for %s", fname)
			}
		}
	} else {
		select {
		case err = <-ch:
		case <-time.After(2 * time.Second):
			return fmt.Errorf("replication timed out for %s", fname)
		}
	}

	return err
}

// ReplicateFilesToSuccessors replicates a list of files to successors
func ReplicateFilesToSuccessors(files []*FileInfo) {
	if len(files) == 0 {
		return
	}

	for _, file := range files {
		fmt.Println("Replicating file:", file.Name)

		fileStateMutex.Lock()

		repInfo := *file

		blocksToReplicate := make([]*FileBlock, len(fileBlockMap[file.Name]))
		copy(blocksToReplicate, fileBlockMap[file.Name])

		fileStateMutex.Unlock()

		repInfo.IsPrimary = false
		repInfo.MostRecent = false
		repInfo.Replicated = true
		PerformReplication(Message{Kind: CREATE, Data: string(MustMarshal(repInfo))}, true)

		for _, block := range blocksToReplicate {
			repBlock := *block
			repBlock.Replicated = true
			PerformReplication(Message{Kind: APPEND, Data: string(MustMarshal(repBlock))}, true)
		}
	}
}

// MustMarshal is a helper that marshals a struct or panics
func MustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// GetFilesOnNode returns all files stored on this node
func GetFilesOnNode() []string {
	fileStateMutex.RLock()
	defer fileStateMutex.RUnlock()

	filenames := make([]string, 0, len(fileInfoMap))
	for name := range fileInfoMap {
		if localBlockCount(name) > 0 {
			filenames = append(filenames, name)
		}
	}
	return filenames
}

func AppendLocalFileToHDFSOnNode(localFile, hdfsFile, vmIP string) error {
	content, err := os.ReadFile(localFile)
	if err != nil {
		return err
	}

	appendBlock := FileBlock{Name: hdfsFile, Content: content, BlockID: -1, Replicated: false}
	appendMsg := Message{Kind: APPEND, Data: string(MustMarshal(appendBlock))}

	nodeID, ok := GetNodeIDFromIP(vmIP)
	if ok {
		member, ok := GetMember(nodeID)
		if !ok {
			return fmt.Errorf("member %s (from IP %s) not in list", nodeID, vmIP)
		}
		return SendMessage(member, appendMsg)
	}

	fmt.Printf("Warning: Node %s not in membership list, attempting direct IP append.\n", vmIP)
	tempMember := MemberInfo{Host: vmIP}
	return SendMessageToMember(tempMember, appendMsg)
}

func ReplicateFilesWrapper(files []*FileInfo) {
	if len(files) == 0 {
		return
	}

	fmt.Printf("[Replication] Starting re-replication for %d files...\n", len(files))
	ReplicateFilesToSuccessors(files)
	fmt.Println("[Replication] Re-replication completed.")
}

func markSeen(file string, blockID int) bool {
	m, ok := blockSeenMap[file]
	if !ok {
		m = map[int]bool{}
		blockSeenMap[file] = m
	}
	if m[blockID] {
		return true
	}
	m[blockID] = true
	return false
}

func ProcessCreateMessage(msg Message, isTemp bool) error {
	fileStateMutex.Lock()
	defer fileStateMutex.Unlock()

	var fi FileInfo
	if err := decodeFileInfo(msg.Data, &fi); err != nil {
		return fmt.Errorf("CREATE: failed to decode file info: %w", err)
	}
	if fi.Name == "" {
		return fmt.Errorf("CREATE: empty file name")
	}

	fmt.Printf("[NODE %s] Start CREATE: %s (Temp: %v)\n", NODE_ID, fi.Name, isTemp)
	if !isTemp {
		var incoming FileInfo
		_ = decodeFileInfo(msg.Data, &incoming)

		if _, exists := fileInfoMap[fi.Name]; exists && !incoming.Replicated {
			return fmt.Errorf("CREATE: file %s already exists", fi.Name)
		}
		if _, exists := tempFileInfoMap[fi.Name]; exists && !incoming.Replicated {
			return fmt.Errorf("CREATE: file %s already exists (temp)", fi.Name)
		}

		if st, err := os.Stat(STORAGE_LOCATION + fi.Name); err == nil && st.IsDir() && !incoming.Replicated {
			return fmt.Errorf("CREATE: file %s already exists (disk)", fi.Name)
		}
	}

	infoMap := fileInfoMap
	if isTemp {
		infoMap = tempFileInfoMap
	}

	init := fi

	if existing, ok := infoMap[fi.Name]; ok {
		init.NumBlocks = existing.NumBlocks
	}

	infoMap[fi.Name] = &init

	dir := STORAGE_LOCATION + fi.Name
	if isTemp {
		dir += "_temp"
	}
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return fmt.Errorf("CREATE: failed to create dir %s: %w", dir, err)
	}
	fmt.Printf("[NODE %s] End CREATE (Processing): %s\n", NODE_ID, fi.Name)
	fmt.Printf("Replica %s: CREATE %s completed (temp=%v)\n", NODE_ID, fi.Name, isTemp)

	if !isTemp && !fi.Replicated && GetPrimaryReplicaNode(fi.Name) == NODE_ID {

		repInfo := fi
		repInfo.Replicated = true
		repInfo.IsPrimary = false

		repMsg := Message{Kind: CREATE, Data: string(MustMarshal(repInfo))}

		err := PerformReplication(repMsg, true)

		if err != nil {
			fmt.Printf("[NODE %s] CREATE replication failed for %s: %v\n", NODE_ID, fi.Name, err)
		}
	}

	return nil
}

func ProcessAppendMessage(msg Message, isTemp bool) error {

	var blk FileBlock
	if err := decodeFileBlock(msg.Data, &blk); err != nil {
		return fmt.Errorf("APPEND: failed to decode block: %w", err)
	}
	if blk.Name == "" {
		return fmt.Errorf("APPEND: empty file name")
	}

	if GetPrimaryReplicaNode(blk.Name) != NODE_ID && !blk.Replicated && !isTemp {
		primary := GetPrimaryReplicaNode(blk.Name)
		if primary == "" {
			return fmt.Errorf("APPEND: no primary for %q", blk.Name)
		}
		member, ok := GetMember(primary)
		if !ok {
			return fmt.Errorf("APPEND: could not find primary member %s", primary)
		}
		ch := make(chan error, 1)
		go SendAnyReplicationMessage(member, msg, ch)
		return <-ch
	}

	fileStateMutex.Lock()
	defer fileStateMutex.Unlock()
	fmt.Printf("[NODE %s] Start APPEND: %s (BlockID: %d, Temp: %v)\n", NODE_ID, blk.Name, blk.BlockID, isTemp)
	infoMap := fileInfoMap
	blockMap := fileBlockMap
	if isTemp {
		infoMap = tempFileInfoMap
		blockMap = tempFileBlockMap
	}
	fi, ok := infoMap[blk.Name]
	if !ok {

		fi = &FileInfo{
			Name:       blk.Name,
			NumBlocks:  0,
			IsPrimary:  false,
			MostRecent: false,
			Replicated: blk.Replicated,
		}
		infoMap[blk.Name] = fi

		dir := STORAGE_LOCATION + blk.Name
		if isTemp {
			dir += "_temp"
		}
		if err := os.MkdirAll(dir, 0o777); err != nil {
			return fmt.Errorf("APPEND: auto-create dir %s: %w", dir, err)
		}
		fmt.Printf("[NODE %s] APPEND auto-created FileInfo for %s\n", NODE_ID, blk.Name)
	} else if fi.Name == "" {
		fi.Name = blk.Name
	}

	fmt.Printf("[NODE %s] Start APPEND: %s (BlockID: %d)\n", NODE_ID, blk.Name, blk.BlockID)

	if GetPrimaryReplicaNode(blk.Name) == NODE_ID && blk.BlockID < 0 && !blk.Replicated {
		blk.BlockID = len(fileBlockMap[fi.Name])
	}

	if blk.BlockID < 0 && !(GetPrimaryReplicaNode(blk.Name) == NODE_ID && !blk.Replicated) {
		return fmt.Errorf("APPEND: missing blockID at non-primary for %s", blk.Name)
	}

	if blk.BlockID >= 0 {
		if markSeen(blk.Name, blk.BlockID) {
			return nil
		}
	}

	blockMap[blk.Name] = append(blockMap[blk.Name], &blk)

	dir := STORAGE_LOCATION + blk.Name
	if isTemp {
		dir += "_temp"
	}
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return fmt.Errorf("APPEND: mkdir %s: %w", dir, err)
	}
	path := fmt.Sprintf("%s/%d", dir, blk.BlockID)
	if err := os.WriteFile(path, blk.Content, 0o644); err != nil {
		return fmt.Errorf("APPEND: write %s: %w", path, err)
	}

	next := blk.BlockID + 1
	if fi.NumBlocks < next {
		fi.NumBlocks = next
	}

	if !blk.Replicated && GetPrimaryReplicaNode(blk.Name) == NODE_ID {
		fi.MostRecent = false
	}

	if blk.Replicated {
		return nil
	}

	if GetPrimaryReplicaNode(blk.Name) == NODE_ID {
		rep := blk
		rep.Replicated = true
		repMsg := Message{Kind: APPEND, Data: string(MustMarshal(rep))}

		err := PerformReplication(repMsg, true)

		if err != nil {
			return fmt.Errorf("APPEND: replication failed: %w", err)
		}
	}
	fmt.Printf("[NODE %s] End APPEND: %s (BlockID: %d)\n", NODE_ID, blk.Name, blk.BlockID)
	return nil
}
