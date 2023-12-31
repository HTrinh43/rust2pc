//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::thread;
use std::sync::Mutex;

use participant::rand::Rng;
use participant::rand::prelude::*;
use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::TryRecvError;
use participant::ipc_channel::ipc::IpcSender as Sender;

use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

///
/// ParticipantState
/// enum for Participant 2PC state machine
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    AwaitingGlobalDecision,
}

///
/// Participant
/// Structure for maintaining per-participant state and communication/synchronization objects to/from coordinator
///
#[derive(Debug)]
pub struct Participant {
    id_str: String,
    state: ParticipantState,
    log: oplog::OpLog,
    running: Arc<AtomicBool>,
    send_success_prob: f64,
    operation_success_prob: f64,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,
    abort: u32,
    commit: u32,
    unknown: u32
}

///
/// Participant
/// Implementation of participant for the 2PC protocol
/// Required:
/// 1. new -- Constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- Implements participant side protocol for 2PC
///
impl Participant {

    ///
    /// new()
    ///
    /// Return a new participant, ready to run the 2PC protocol with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course.
    ///
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>) -> Participant {

        Participant {
            id_str: id_str,
            state: ParticipantState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r,
            send_success_prob: send_success_prob,
            operation_success_prob: operation_success_prob,
            // TODO
            tx,
            rx,
            abort : 0,
            commit: 0,
            unknown: 0
        }
    }


    ///
    /// send()
    /// Send a protocol message to the coordinator. This can fail depending on
    /// the success probability. For testing purposes, make sure to not specify
    /// the -S flag so the default value of 1 is used for failproof sending.
    ///
    /// HINT: You will need to implement the actual sending
    ///

    pub fn send(&mut self, pm: ProtocolMessage)  {
    let mut rng = rand::thread_rng();  // Get a random number generator
    let x: f64 = rng.gen();
    let mut mes = pm.clone();
    mes.senderid = self.id_str.clone();
    if  x <= self.send_success_prob {
        if pm.mtype == MessageType::ParticipantVoteCommit{
            self.commit += 1;
        } else {
            self.abort += 1;
        }
        let _ = self.tx
            .send(pm)
            .map_err(|e| format!("Failed to send message: {}", e));
    } else {
        self.unknown+=1;
    }
}


    ///
    /// perform_operation
    /// Perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the
    /// command-line option success_probability.
    ///
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic.
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than
    ///       bool if it's more convenient for your design).
    ///

    pub fn perform_operation(&mut self, request_option: Option<ProtocolMessage>) -> bool {
        trace!("{}::Performing operation", self.id_str.clone());
        if let Some(message) = request_option {
            let mut rng = rand::thread_rng();  // Get a random number generator
            let x: f64 = rng.gen();
            if x <= self.operation_success_prob {
                self.log.append(MessageType::ParticipantVoteCommit, message.txid.clone(), message.senderid.clone(), message.opid);
                true
            } else {
                // Log failure, take necessary steps for operation failure.
                // self.log.log_failure(&request);
                self.log.append(MessageType::ParticipantVoteAbort, message.txid.clone(), message.senderid.clone(), message.opid);
                false
            }
        } else {
            // If there is no operation request, do nothing and return false.
            false
        }
    }


    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///


    pub fn report_status(&mut self) {
        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.id_str.clone(), self.commit, self.abort, self.unknown);
    }



    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    // pub fn wait_for_exit_signal(&mut self) {
    //     trace!("{}::Waiting for exit signal", self.id_str.clone());

    //     // TODO

    //     trace!("{}::Exiting", self.id_str.clone());
    // }

    pub fn wait_for_exit_signal(&self) {
        while self.running.load(Ordering::SeqCst) {
            // Sleep for a short duration to avoid busy waiting.
            thread::sleep(Duration::from_millis(100));
        }
        trace!("{}::Exiting", self.id_str.clone());
    }


    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        trace!("{}::Beginning protocol", self.id_str.clone());
        // TODO
        let timeout_duration = Duration::from_secs(3);
        let mut start = Instant::now();
        loop {
            if !self.running.load(Ordering::SeqCst) {
                trace!("{}::Exiting", self.id_str.clone());
                break;
            }
            match self.rx.recv() {
                Ok(message) => {
                    match message.mtype {
                        MessageType::CoordinatorPropose => {
                            let mut mes = message.clone();
                            if self.perform_operation(Some(message.clone())){
                                mes.mtype = MessageType::ParticipantVoteCommit;
                            } else {
                                mes.mtype = MessageType::ParticipantVoteAbort;
                            }
                            self.send(mes);
                            // self.tx.send(mes).expect("Failed to send participant vote");
                        },
                        MessageType::CoordinatorCommit => {
                        },
                        MessageType::CoordinatorAbort => {
                        },
                        MessageType::CoordinatorExit =>{
                            break;
                        } 
                        _ => {
                            // Handle all other message types
                            let mess = format!("{:?}", message);
                            println!("{}", mess);
                        }
                    }
                    self.log.append(message.mtype.clone(), message.txid.clone(), message.senderid.clone(), message.opid);
                    start = Instant::now();
                },
                Err(e) => {
                    // println!("Error:{:?}",e);
                    break;
                }
            }
        // self.wait_for_exit_signal();
        }
        self.report_status();
    }
}
