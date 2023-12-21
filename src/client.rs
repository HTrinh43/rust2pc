//!
//! client.rs
//! Implementation of 2PC client
//!
extern crate ipc_channel;
extern crate log;
extern crate stderrlog;

use std::thread;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;

use client::ipc_channel::ipc::IpcReceiver as Receiver;
use client::ipc_channel::ipc::TryRecvError;
use client::ipc_channel::ipc::IpcSender as Sender;

use message;
use message::MessageType;
use message::RequestStatus;
use message::ProtocolMessage;

// Client state and primitives for communicating with the coordinator
#[derive(Debug)]
pub struct Client {
    pub id_str: String,
    pub running: Arc<AtomicBool>,
    tx: Sender<ProtocolMessage>,
    rx: Mutex<Receiver<ProtocolMessage>>,
    pub num_requests: u32,
    pub successful_ops: u32,  // Add this line
    pub failed_ops: u32,      // Add this line
    pub unknown_ops: u32, 
    op: u32
}

///
/// Client Implementation
/// Required:
/// 1. new -- constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown
/// 3. pub fn protocol(&mut self, n_requests: i32) -- Implements client side protocol
///
impl Client {

    ///
    /// new()
    ///
    /// Constructs and returns a new client, ready to run the 2PC protocol
    /// with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    ///
    pub fn new(id_str: String,
               running: Arc<AtomicBool>,
               tx: Sender<ProtocolMessage>,
               rx: Receiver<ProtocolMessage>,
               n_requests: u32) -> Client {
        Client {
            id_str: id_str,
            running: running,
            tx: tx,
            rx: Mutex::new(rx), 
            num_requests: n_requests, 
            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
            op: 0
        }
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        // TODO
        while self.running.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(100));
        }
        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// send_next_operation(&mut self)
    /// Send the next operation to the coordinator
    ///
    pub fn send_next_operation(&mut self) {

        // Create a new request with a unique TXID.
        self.op = self.op + 1;
        let txid = format!("{}_op_{}", self.id_str.clone(), self.op);

        let pm = message::ProtocolMessage::generate(message::MessageType::ClientRequest,
                                                    txid.clone(),
                                                    self.id_str.clone(),
                                                    self.op);
        info!("{}::Sending operation #{}", self.id_str.clone(), self.op);

        // TODO
        self.tx.send(pm).expect("Failed to send operation");
        trace!("{}::Sent operation #{}", self.id_str.clone(), self.op);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the
    /// last issued request. Note that we assume the coordinator does
    /// not fail in this simulation
    ///
    pub fn recv_result(&mut self) {

        info!("{}::Receiving Coordinator Result", self.id_str.clone());

        // TODO
        if let Ok(rx) = self.rx.lock() {

            loop {
                match rx.try_recv() {
                    Ok(message) => {
                        match message.mtype {
                            MessageType::ClientResultCommit => self.successful_ops += 1,
                            MessageType::ClientResultAbort => self.failed_ops += 1,
                            MessageType::CoordinatorExit => self.running.store(false, Ordering::SeqCst),
                            _ => {
                                // Handle all other message types
                            }
                        }
                        // let mess = format!("Received result {:?}", message);
                        // println!("{}", mess);
                        break;
                    },
                    Err(_e) => {
                        trace!("Client receive error.");
                    }
                } 
               
            }
        }
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this client before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}", 
                 self.id_str, self.successful_ops, self.failed_ops);
    }

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    ///
    pub fn protocol(&mut self, n_requests: u32) {
        // TODO
        for _ in 0..n_requests {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            self.send_next_operation();
            self.recv_result();
            // This sleep is to prevent bombarding the coordinator too quickly.
            thread::sleep(Duration::from_millis(100));
        }
        self.report_status();
    }
}
