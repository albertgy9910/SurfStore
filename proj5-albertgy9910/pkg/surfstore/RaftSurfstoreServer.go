package surfstore

import (
	context "context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64
	nextIndex      []int64
	matchIndex     []int64
	logMutex       *sync.RWMutex

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	count := 1
	responses := make(chan bool, len(s.peers)-1)
	for index, addr := range s.peers {
		if int64(index) == s.id {
			continue
		}
		go s.checkSurvival(addr, responses)
	}

	for {
		success := <-responses
		if success {
			count++
		}
		if count > len(s.peers)/2 {
			return s.metaStore.GetFileInfoMap(ctx, empty)
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	count := 1
	responses := make(chan bool, len(s.peers)-1)
	for index, addr := range s.peers {
		if int64(index) == s.id {
			continue
		}
		go s.checkSurvival(addr, responses)
	}

	for {
		success := <-responses
		if success {
			count++
		}

		if count > len(s.peers)/2 {
			return s.metaStore.GetBlockStoreMap(ctx, hashes)
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	count := 1
	responses := make(chan bool, len(s.peers)-1)
	for index, addr := range s.peers {
		if int64(index) == s.id {
			continue
		}
		go s.checkSurvival(addr, responses)
	}

	for {
		success := <-responses
		if success {
			count++
		}

		if count > len(s.peers)/2 {
			return s.metaStore.GetBlockStoreAddrs(ctx, empty)
		}
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	fmt.Println("Update")
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	s.logMutex.Lock()
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	s.logMutex.Unlock()

	commitChannel := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChannel)
	// send to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx, commitChannel)

	commit := <-commitChannel
	if commit {
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
		s.lastApplied++
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
	return nil, nil
}

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	output := &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}
	if input.Term > s.term {
		s.isLeaderMutex.RLock()
		s.isLeader = false
		s.isLeaderMutex.RUnlock()
		s.term = input.Term
	}
	if len(input.Entries) > 0 {
		if int64(len(s.log)) <= input.PrevLogIndex {
			output.MatchedIndex = int64(len(s.log) - 1)
			return output, nil
		}
		// 1. Reply false if term < currentTerm (§5.1)
		if input.Term < s.term {
			return output, nil
		}
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
		// matches prevLogTerm (§5.3)

		if input.PrevLogIndex != -1 {
			if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
				for i := input.PrevLogIndex - 1; i >= 0; i-- {
					if s.log[i].Term != s.log[input.PrevLogIndex].Term {
						output.MatchedIndex = i
						break
					}
				}
				return output, nil
			}
		}

		output.Success = true
		// 3. If an existing entry conflicts with a new one (same index but different
		// terms), delete the existing entry and all that follow it (§5.3)
		s.logMutex.Lock()
		for index, entry := range s.log {
			if entry.Term != input.Entries[index].Term {
				s.log = s.log[:index]
				break
			}
		}
		// 4. Append any new entries not already in the log
		for index := len(s.log); index < len(input.Entries); index++ {
			s.log = append(s.log, input.Entries[index])
		}
		s.logMutex.Unlock()
		output.MatchedIndex = int64(len(input.Entries) - 1)
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	// of last new entry)
	if input.LeaderCommit > s.commitIndex {
		if input.LeaderCommit >= int64(len(s.log))-1 {
			s.commitIndex = int64(len(s.log)) - 1
		} else {
			s.commitIndex = input.LeaderCommit
		}
	}

	// Update
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}
	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("Leader")
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()

	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()

	s.term++

	for index := range s.peers {
		s.nextIndex[index] = int64(len(s.log))
		s.matchIndex[index] = -1
	}
	for _, ch := range s.pendingCommits {
		if *ch != nil {
			*ch <- false
		}
	}
	s.pendingCommits = make([]*chan bool, len(s.log))
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("Heartbeat")
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	dummy := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	if len(s.log) != 0 {
		dummy.PrevLogIndex = int64(len(s.log) - 1)
		dummy.PrevLogTerm = s.log[len(s.log)-1].Term
	}
	for index, addr := range s.peers {
		if index == int(s.id) {
			continue
		}
		dummy.PrevLogIndex = int64(s.nextIndex[index] - 1)
		if s.nextIndex[index] != 0 {
			dummy.PrevLogTerm = s.log[s.nextIndex[index]-1].Term
		} else {
			dummy.PrevLogTerm = -1
		}
		conn, _ := grpc.Dial(addr, grpc.WithInsecure())
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		output, err := c.AppendEntries(ctx, &dummy)
		if err == nil {
			if output.Success {
				s.nextIndex[index] = int64(len(s.log))
				s.matchIndex[index] = int64(len(s.log) - 1)
			} else if output.Term > s.term {
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.term = output.Term
				s.isLeaderMutex.Unlock()
			} else {
				s.nextIndex[index] = output.MatchedIndex + 1
			}
		}
	}
	lastIndex := s.commitIndex
	for i := s.commitIndex + 1; i < int64(len(s.log)); i++ {
		count := 1
		for index := range s.peers {
			if s.matchIndex[index] >= i && int64(index) != s.id {
				count++
				if count > len(s.peers)/2 {
					if s.log[i].Term == s.term {
						s.commitIndex = i
					}
					break
				}
			}
		}
	}
	for i := lastIndex + 1; i <= s.commitIndex; i++ {
		if s.pendingCommits[i] == nil {
			break
		}
		*s.pendingCommits[i] <- true
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) checkSurvival(addr string, responses chan bool) {
	for {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}

		c := NewRaftSurfstoreClient(conn)
		dummy := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  0,
			PrevLogIndex: 0,
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err = c.AppendEntries(ctx, dummy)
		conn.Close()
		if err == nil {
			responses <- true
			return
		} else {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
		}
	}
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context, commitChannel chan bool) {
	fmt.Println("sendToAllFollowersInParallel")
	newIndex := s.commitIndex + 1
	count := 1
	responses := make(chan bool, len(s.peers)-1)
	for index, addr := range s.peers {
		if int64(index) == s.id {
			continue
		}
		go s.sendToFollowers(index, addr, ctx, responses)
	}

	for {
		if s.isCrashed {
			return
		}

		res := <-responses
		if res {
			count++
		}
		// majority agree
		if count > len(s.peers)/2 {
			s.commitIndex = newIndex
			commitChannel <- true
			break
		}
	}

	for count < len(s.peers) {
		res := <-responses
		if res {
			count++
		}
	}
}

func (s *RaftSurfstore) sendToFollowers(index int, addr string, ctx context.Context, responses chan bool) {
	fmt.Println("sendToFollowers")
	if s.isCrashed {
		responses <- false
		return
	}

	var prevLogTerm int64
	if s.nextIndex[index] != 0 {
		prevLogTerm = s.log[s.nextIndex[index]-1].Term
	} else {
		prevLogTerm = -1
	}

	prevLog := s.nextIndex[index] - 1

	input := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLog,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	c := NewRaftSurfstoreClient(conn)

	res, err := c.AppendEntries(ctx, &input)
	conn.Close()

	if err != nil {
		if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
			return
		}
	}

	if res != nil {
		if res.Success {
			responses <- true
			s.nextIndex[index] = int64(len(s.log))
			s.matchIndex[index] = int64(len(s.log) - 1)
		} else if res.Term > s.term {
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.term = res.Term
			s.isLeaderMutex.Unlock()
		} else {
			s.nextIndex[index] = res.MatchedIndex + 1
		}
	}

}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
