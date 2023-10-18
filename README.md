# Raft Implment

# C**ontents**

# Intro

- Implement in raft/raft.go

## Code Preview

- Make(peers,me,…): create a Raft peer
    - me: index of this peer in the peers array
- Start(command): asks Raft to start the processing to append the command to the replicated log
    - Return immediately
    - ApplyMsg
- raft.go
    - sendRequestVote()
    - RequestVote()
    - rpc package → src/labrpc

# Topic

## **Part 2A**

### Task

- [x]  Implement Raft leader election
- [x]  Implement heartbeats

### Goal

- [x]  a single leader to be elected
- [x]  the leader to remain the leader if there are no failures
- [x]  a new leader to take over
    - [x]  if the old leader fails
    - [x]  if packets to/from the old leader are lost

### Hint

- Test
    
    ```jsx
    go test -run 2A
    ```
    
- Follow the paper's Figure 2. At this point you care about sending and receiving RequestVote RPCs, the Rules for Servers that relate to elections, and the State related to leader election
- Fill in
    - RequestVoteArgs
    - RequestVoteReply
- Modify
    - Make(): create a background goroutine that will kick off leader election periodically by sending out
    - Fill in the RequestVoteArgs and RequestVoteReply structs. Modify Make() to create a background goroutine that will kick off leader election periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while. This way a peer will learn who is the leader, if there is already a leader, or become the leader itself. Implement the RequestVote() RPC handler so that servers will vote for one another.
- Heartbeats
    - To implement heartbeats, define an AppendEntries RPC struct (though you may not need all the arguments yet), and have the leader send them out periodically. Write an AppendEntries RPC handler method that resets the election timeout so that other servers don't step forward as leaders when one has already been elected.
- Read this advice about [locking](http://nil.csail.mit.edu/6.824/2020/labs/raft-locking.txt) and [structure](http://nil.csail.mit.edu/6.824/2020/labs/raft-structure.txt).
- Timeout:
    - The tester requires that the leader send heartbeat RPCs no more than ten times per second.
    - The tester requires your Raft to elect a new leader within five seconds of the failure of the old leader (if a majority of peers can still communicate).
    - paper: 150 ~ 300 ms
    - tester: 5 s > 300 ms
- Random: [rand](https://golang.org/pkg/math/rand/)
- Sleep: [time.Sleep()](https://golang.org/pkg/time/#Sleep)
- Print
    - util.go → DPrintf
- Check
    
    ```jsx
    go test -race
    ```
    

## **Part 2B**

- RequestVote RPC
    - lastLogIndex
    - lastLogTerm
- Leader
    - nextIndex
    - matchIndex

# Implement

## State

### Follower

- heartbeat timeout → Candidate
- vote (each term)

### Candidate

### Leader

## Reconnection

- cur Leader Term < Node Term

## 2A

1. ticker → election
2. heartbeater → heartbeat

# Log Commit

1. Normal commit
    1. Leader success to commit log to most of followers
    2. Leader update LeaderCommit
2. Conflict log
3. 

## RequestVote

## 2A

1. Candidate to Follower
2. Candidate to Candidate

## 2B

1. Check raft node commit to local → update LeaderCommit
2. Check LastLogIndex and LastLogTerm
3. Follower get heartbeat from leader → update CommitIndex
4. Follower reply missing → Leader resend missing log

# Structure

- [ ]  Refactor send RPC to peers

# Log format

- Peer number
- Term
- action

# Note

- Done
    - [x]  implement election record
    - [x]  heartbeater → reset election timer
    - [x]  increase currentTerm in each triggerElection
    - [x]  each rpc request had timeout
        - RPC_TIMEOUT_SEC
    - [x]  ticker function
    - [x]  merge triggerHeartbeat and sendRPC2Peers
    - after getting voted from k node, do not send vote request again
    - replace term after voted or obtained heartbeat
    - the election timeouts are chosen from a range between 10–20 times the cluster’s one-way network latency
- [ ]  prevote state
- [ ]  Joint Consensus Algorithm
- Leader only send current entry to followers

# Issue

- Node don’t trigger election after Leader is disconnect
    - time ticker
- mutiple goroutine in trigger*
- [ ]  Change Log
- How to re-send missing enties

# Test Command

## 2A

- Test for single time
    
    ```go
    go test --run TestInitialElection2A > ./log/2A/TestInitialElection2A.log
    go test --run TestReElection2A > ./log/2A/TestReElection2A.log
    go test --run TestManyElections2A > ./log/2A/TestManyElections2A.log
    ```
    
- Test for multiple time
    
    ```go
    go test --failfast --count 30 --run TestInitialElection2A > ./log/2A/TestInitialElection2A.log
    go test --failfast --count 30 --run TestReElection2A > ./log/2A/TestReElection2A.log
    go test --failfast --count 30 --run TestManyElections2A > ./log/2A/TestManyElections2A.log
    ```
    

## 2B

- Test for single time
    
    ```go
    go test --run TestBasicAgree2B > ./log/TestBasicAgree2B.log
    go test --run TestRPCBytes2B > ./log/TestRPCBytes2B.log
    go test --run TestFailAgree2B > ./log/TestFailAgree2B.log
    ```
    
- Test for multiple time
    
    ```go
    go test --failfast --count 30 --run TestBasicAgree2B > ./log/TestBasicAgree2B.log
    go test --failfast --count 30 --run TestRPCBytes2B > ./log/TestRPCBytes2B.log
    go test --failfast --count 30 --run TestFailAgree2B > ./log/TestFailAgree2B.log
    ```
    

# Github flow

- with test

# Ref

[https://github.com/kophy/6.824](https://github.com/kophy/6.824)

[6.824 Lab 2: Raft](http://nil.csail.mit.edu/6.824/2020/labs/lab-raft.html)

[Raft Consensus Algorithm](https://raft.github.io/)

[](http://wcl.cs.rpi.edu/pilots/library/papers/consensus/RAFTOngaroPhD.pdf)

[Read-Write Quorum System 及在 Raft 中的實踐](https://www.readfog.com/a/1672444744733659136)

[https://github.com/etcd-io/etcd](https://github.com/etcd-io/etcd)