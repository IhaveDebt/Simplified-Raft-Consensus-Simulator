//
// RaftSim.swift
// Simplified Raft consensus protocol simulator for leader election + log replication (toy).
// This sim is single-process, simulating message passing between node structs.
// Useful to study behaviors like leader election timeouts, heartbeats, and log replication.
// Swift 5+
//

import Foundation

enum Role { case follower, candidate, leader }

struct LogEntry {
    let term: Int
    let command: String
}

class Node {
    let id: Int
    var role: Role = .follower
    var currentTerm: Int = 0
    var votedFor: Int? = nil
    var logs: [LogEntry] = []
    var commitIndex: Int = -1
    var lastApplied: Int = -1
    
    // volatile leader state
    var nextIndex: [Int: Int] = [:]
    var matchIndex: [Int: Int] = [:]
    
    // election timers
    var electionTimeout: TimeInterval = 0
    var lastHeartbeat: TimeInterval = 0
    
    init(id: Int) { self.id = id }
}

class Network {
    var nodes: [Node] = []
    var time: TimeInterval = 0
    
    init(n: Int) {
        for i in 0..<n { nodes.append(Node(id: i)) }
        resetElectionTimers()
    }
    
    func resetElectionTimers() {
        for n in nodes {
            n.electionTimeout = TimeInterval.random(in: 1.0...2.0)
            n.lastHeartbeat = time
            n.role = .follower
            n.votedFor = nil
        }
    }
    
    func tick(_ dt: TimeInterval) {
        time += dt
        // check followers for election timeout
        for n in nodes {
            switch n.role {
                case .leader:
                    // send heartbeat
                    if time - n.lastHeartbeat > 0.5 {
                        n.lastHeartbeat = time
                        broadcastHeartbeat(from: n)
                    }
                case .follower:
                    if time - n.lastHeartbeat >= n.electionTimeout {
                        startElection(candidate: n)
                    }
                case .candidate:
                    if time - n.lastHeartbeat >= n.electionTimeout {
                        startElection(candidate: n)
                    }
            }
        }
    }
    
    func broadcastHeartbeat(from leader: Node) {
        // leader sends AppendEntries (heartbeat) to all others
        for target in nodes where target.id != leader.id {
            // follower accepts heartbeat, updates lastHeartbeat and term if needed
            if target.currentTerm <= leader.currentTerm {
                target.currentTerm = leader.currentTerm
                target.lastHeartbeat = time
                target.role = .follower
                target.votedFor = nil
            } else {
                // leader sees higher term -> step down
                if leader.currentTerm < target.currentTerm {
                    leader.role = .follower
                }
            }
        }
        print("[t=\(String(format: "%.2f", time))] Leader \(leader.id) sent heartbeat")
    }
    
    func startElection(candidate: Node) {
        candidate.role = .candidate
        candidate.currentTerm += 1
        candidate.votedFor = candidate.id
        candidate.lastHeartbeat = time
        candidate.electionTimeout = TimeInterval.random(in: 1.0...2.0)
        print("[t=\(String(format: "%.2f", time))] Node \(candidate.id) starts election term \(candidate.currentTerm)")
        // request votes
        var votes = 1
        for other in nodes where other.id != candidate.id {
            // if other hasn't voted this term, or candidate has higher term
            if other.votedFor == nil || other.votedFor == candidate.id {
                other.votedFor = candidate.id
                other.lastHeartbeat = time
                votes += 1
            }
        }
        if votes > nodes.count / 2 {
            // becomes leader
            candidate.role = .leader
            print("[t=\(String(format: "%.2f", time))] Node \(candidate.id) became leader with \(votes) votes")
            // init leader state
            for n in nodes { candidate.nextIndex[n.id] = candidate.logs.count; candidate.matchIndex[n.id] = 0 }
        } else {
            print("[t=\(String(format: "%.2f", time))] Node \(candidate.id) got \(votes) votes (failed)")
        }
    }
    
    func proposeCommand(_ cmd: String) {
        // leader appends and replicates
        guard let leader = nodes.first(where: { $0.role == .leader }) else {
            print("No leader to propose command")
            return
        }
        leader.logs.append(LogEntry(term: leader.currentTerm, command: cmd))
        print("Leader \(leader.id) appended command '\(cmd)' at index \(leader.logs.count-1)")
        // replicate (simplified synchronous replication)
        var majority = 1
        for n in nodes where n.id != leader.id {
            // accept append
            n.logs.append(LogEntry(term: leader.currentTerm, command: cmd))
            majority += 1
        }
        if majority > nodes.count/2 {
            leader.commitIndex = leader.logs.count-1
            print("Command committed at index \(leader.commitIndex)")
        }
    }
}

// -----------------------------
// Demo runner
// -----------------------------
func demoRaftSim() {
    print("=== RaftSim Demo ===")
    let net = Network(n: 5)
    var simTime: TimeInterval = 0
    let dt: TimeInterval = 0.2
    while simTime < 10 {
        net.tick(dt)
        simTime += dt
        // after some time, if leader exists, propose commands
        if simTime > 3 {
            if simTime.truncatingRemainder(dividingBy: 2.0) < 0.001 {
                net.proposeCommand("set x = \(Int(simTime))")
            }
        }
        Thread.sleep(forTimeInterval: dt)
    }
}

demoRaftSim()
