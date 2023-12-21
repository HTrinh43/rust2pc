//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;
use ipc_channel::ipc::IpcOneShotServer;
use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision
}

/// Coordinator
/// Struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
    num_request: u32,
    participants :HashMap<String, Sender<ProtocolMessage>>,
    clients:HashMap<String, Sender<ProtocolMessage>>,
    client_rx: Receiver<ProtocolMessage>,
    participant_rx: Receiver<ProtocolMessage>,
    global_commit: u32,
    commit: u32,
    global_abort: u32,
    abort : u32,
    unknown: u32
}

///
/// Coordinator
/// Implementation of coordinator functionality
/// Required:
/// 1. new -- Constructor
/// 2. protocol -- Implementation of coordinator side of protocol
/// 3. report_status -- Report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- What to do when a participant joins
/// 5. client_join -- What to do when a client joins
///
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     log_path: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///
    pub fn new(
        log_path: String,
        r: &Arc<AtomicBool>,
        num_request: u32,
        client_rx: Receiver<ProtocolMessage>,
        participant_rx: Receiver<ProtocolMessage>) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            // TODO
            participants: HashMap::new(),
            clients: HashMap::new(),
            num_request,
            client_rx,
            participant_rx,
            global_commit: 0,
            global_abort: 0,
            commit: 0,
            abort: 0,
            unknown: 0
        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self, name: &String, tx: Sender<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);
        // if self.participants.contains_key(name) {
        //     return Err("Participant already exists".to_string());
        // }
        // TODO
        // Store the sender and receiver in the participants HashMap.
        self.participants.insert(name.to_string(), tx);
        
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(&mut self, name: &String, tx: Sender<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        // Store the client's communication channels.
        self.clients.insert(name.clone(), tx);
       
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        println!("Global\nCommitted: {:6}\tAborted: {:6}\n=======\nCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", 
        self.global_commit,self.global_abort, self.commit, self.abort, self.unknown);
    }



    pub fn receive_client_request(&mut self) {
        let timeout_duration = Duration::from_millis(200);
        let mut start = Instant::now();
        // for (tx, message) in requests {
        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            match self.client_rx.try_recv(){
                Ok(message) => {
                    start = Instant::now();
                    match message.mtype {
                        MessageType::ClientRequest => {
                            // Send prepare messages to all participants
                            // save the client id to send back later
                            let client_id = message.senderid.clone();
                            self.send_prepare_message(&message.clone());
                            // Collect votes from participants
                            let votes = self.collect_votes();
                            // Decide on commit or abort based on votes
                            let decision = if votes.iter().all(|&vote| vote == MessageType::ParticipantVoteCommit) {
                                MessageType::CoordinatorCommit
                            } else {
                                MessageType::CoordinatorAbort
                            };
                            if decision == MessageType::CoordinatorCommit {
                                self.global_commit += 1;
                            } else{
                                self.global_abort += 1;
                            }
                            let mut mes = message.clone();
                            mes.mtype = decision;
                            // println!("Sending out decision {:?}", decision.clone());
                            // Send the decision to all participants
                            self.send_decision_message(mes.clone());
                            // generate client result
                            let  client_decision = if decision ==  MessageType::CoordinatorCommit {
                                MessageType::ClientResultCommit
                            } else {
                                MessageType::ClientResultAbort
                            };
                            let mut client_result = message.clone();
                            client_result.mtype = client_decision;
                            match self.clients.get(&client_id) {
                                Some(&ref tx) => tx.send(client_result).expect("Fail to send client result."),
                                None => println!("No client exists"),
                            }
                        }
                        _ => continue
                    }
                },
                Err(e) => {
                    match e {
                        TryRecvError::Empty => {
                            // The channel is empty, no message to receive at the moment.
                            // Handle the case when there's no message available.
                            if start.elapsed() >= timeout_duration {
                                break;
                            }
                        }
                        TryRecvError::IpcError(_) =>{
                            println!("{:?}", e);
                        }
                    }
                },
            } 
        }
    }

    pub fn send_prepare_message(&mut self, pm: &ProtocolMessage) {
        for (_, tx) in &self.participants {
            // Construct the prepare message
            let message = ProtocolMessage::generate(MessageType::CoordinatorPropose, pm.txid.clone(), pm.senderid.clone(), pm.opid); 
            // Send the message to the participant
            tx.send(message).expect("Failed to send prepare message");

        }
    }

    // Collect votes from all participants
    pub fn collect_votes(&mut self) -> Vec<MessageType> {
        let mut votes = Vec::new();
        let timeout_duration = Duration::from_millis(200);
        let mut start = Instant::now();

        loop {
            // if !self.running.load(Ordering::SeqCst) {
            //     let decision = ProtocolMessage::generate(MessageType::CoordinatorExit,"exit".to_string(), "exit".to_string(), 0);
            //     self.send_decision_message(decision);
            //     break;
            // }
            match self.participant_rx.try_recv() {
                Ok(message) => {

                    // let mess = format!("Received result {:?}", message);
                    // println!("{}", mess);

                    match message.mtype {
                        MessageType::ParticipantVoteCommit | MessageType::ParticipantVoteAbort => {
                            votes.push(message.mtype);
                            start = Instant::now();
                            if message.mtype == MessageType::ParticipantVoteCommit {
                                self.commit+=1;
                            } else {
                                self.abort += 1;
                            }
                            self.log.append(MessageType::ParticipantVoteCommit, message.txid.clone(), message.senderid.clone(), message.opid);
                        }
                        _ => {
                            let mess = format!("{:?}", message);
                            println!("{}", mess);
                            // eprintln!("Unexpected message type during vote collection");
                        }
                    }
                },
                Err(e) => {
                    match e {
                        TryRecvError::Empty => {
                            if votes.len() == self.participants.len(){
                                break;
                            }
                            if start.elapsed() >= timeout_duration {
                                votes.push(MessageType::ParticipantVoteAbort);
                                self.unknown += 1;
                                self.log.append(MessageType::ParticipantVoteAbort, "None".to_string(), "None".to_string(), 0);
                                break;
                            }
                            // println!("coordinator break");
                            // println!("{:?}", start.elapsed());
                        }
                        TryRecvError::IpcError(_) =>{
                        }
                    }
                },
            }
        }
        votes
    }
    // Sends the final decision message (commit or abort) to all participants.
    pub fn send_decision_message(&mut self, decision: ProtocolMessage ) {
        for (_, tx) in &self.participants {
            // Send the decision message to the participant
            if let Err(e) = tx.send(decision.clone()) {
                println!("Failed to send decision message : {}", e);
            }
        }
        self.log.append(decision.mtype.clone(), decision.txid.clone(), decision.senderid.clone(), decision.opid);
    }

    pub fn send_exit_message(&mut self){
        for (_, tx) in &self.participants {
            let message = ProtocolMessage::generate(MessageType::CoordinatorExit, "exit".to_string(), "exit".to_string(), 0);
            // Send the decision message to the participant
            if let Err(e) = tx.send(message.clone()) {
                println!("Failed to send decision message : {}", e);
            }
        }
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {

        // TODO
        self.receive_client_request();
        self.send_exit_message();
        println!("Exit coordinator");
        // The protocol part is over, now report the status
        
    }
}
