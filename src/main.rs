#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::process::{Child,Command,Stdio};
use std::time::{Duration, Instant};
use std::thread;
use std::error::Error;

use ipc_channel::ipc::IpcSender as Sender;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::channel;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use message::ProtocolMessage;
use message::MessageType;
use message::RequestStatus;
use client::Client;
use participant::Participant;
use std::io::Write;
///
/// pub fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     child_opts: CLI options for child process
///
/// 1. Set up IPC
/// 2. Spawn a child process using the child CLI options
/// 3. Do any required communication to set up the parent / child communication channels
/// 4. Return the child process handle and the communication channels for the parent
///
/// HINT: You can change the signature of the function if necessary
///
fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions, mode: &str, num: u32, tx_coor: Sender<ProtocolMessage>) -> (Child, Sender<ProtocolMessage>) {
    let mut opts = child_opts.clone();
    opts.mode = mode.to_string();
    opts.num = num;
    let (server, server_name) = IpcOneShotServer::new().expect("Failed to create IPC one-shot server");
    opts.ipc_path = server_name.clone();

    let child = Command::new(env::current_exe().unwrap())
        .stdin(Stdio::piped())
        .args(opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    // let (tx, rx) = channel().unwrap().expect("Failed to create an IPC channel");
    // TODO
    
    // Send the server name to the child process
    // if let Some(ref mut stdin) = child.stdin {
    // // Now you have a mutable reference to ChildStdin and can call write_all
    //     stdin.write_all(server_name.as_bytes()).expect("Failed to send server name to child");
    // } else {
    //         panic!("Child process stdin has not been captured!");
    // }
    // Receive the initial message from the child
    let (_, (server_name_client,tx_client)): (_ , (String,Sender<ProtocolMessage>)) = server.accept().expect("Failed to accept on IPC one-shot server");
    
    let tx0 = Sender::connect(server_name_client.clone()).expect("Failed to create a new channel");
    tx0.send(tx_coor).expect("Failed to send tx_coor");
    
    thread::sleep(Duration::from_millis(100));

    (child, tx_client)
}

///
/// pub fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     opts: CLI options for this process
///
/// 1. Connect to the parent via IPC
/// 2. Do any required communication to set up the parent / child communication channels
/// 3. Return the communication channels for the child
///
/// HINT: You can change the signature of the function if necessasry
///
fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (tx, rx): (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) = channel().expect("Failed to create a new channel");

    // The coordinator should send us the receiver end of the channel after we connect.
    // Here, we create a one-shot server and immediately wait for the coordinator's response.
    let (one_shot_server, one_shot_name) = IpcOneShotServer::new()
        .expect("Failed to create one-shot IPC server");
    // Connect to the coordinator using the provided IPC path.
    // let tx0 = Sender::connect(opts.ipc_path.clone())
        // .expect("Failed to connect to the coordinator's IPC socket");
    match Sender::connect(opts.ipc_path.clone()) {
        Ok(tx0) => {
            // Connection was successful, you can use tx0 here
            tx0.send((one_shot_name,tx.clone())).expect("Failed to send the one-shot server name to the coordinator");
        },
        Err(e) => {
            eprintln!("Failed to connect to the server: {:?}", e);
        }
    }

    // Send the name of the one-shot server to the coordinator so they can connect to it.
    
    // Wait for the coordinator to send us our receiver.
    let (_, tx) = one_shot_server.accept()
        .expect("Failed to sender the IPC receiver from the coordinator");
    
    (tx, rx)
}


///
/// pub fn run(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Creates a new coordinator
/// 2. Spawns and connects to new clients processes and then registers them with
///    the coordinator
/// 3. Spawns and connects to new participant processes and then registers them
///    with the coordinator
/// 4. Starts the coordinator protocol
/// 5. Wait until the children finish execution
///
fn run(opts: &mut tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");
    // TODO
    let (tx_coor_client, rx_coor_client): (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) = channel().unwrap();
    let (tx_coor_part, rx_coor_part): (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) = channel().unwrap();
    let mut clients = Vec::new();
    let mut participants = Vec::new();
    let mut coordinator = coordinator::Coordinator::new(coord_log_path, &running, opts.num_requests,rx_coor_client,rx_coor_part);
    
    for i in 0..opts.num_clients {
        let client_id_str = format!("client_{}", i); 
        let (child, tx) = spawn_child_and_connect(opts, "client", i, tx_coor_client.clone());
        coordinator.client_join(&client_id_str, tx);
        clients.push(child);
    }
    
    for i in 0..opts.num_participants {
        let participant_id_str = format!("participant_{}", i); // Unique identifier for each participant
        let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str); // Log path for each participant
        let (child, tx) = spawn_child_and_connect(opts, "participant", i,tx_coor_part.clone());
        coordinator.participant_join(&participant_id_str, tx);
        participants.push(child);
    }
    // Start the coordinator protocol
    coordinator.protocol();

    // Wait for child processes to complete
    for mut client in clients {
        client.wait().expect("Failed to wait on client");
    }
    for mut participant in participants {
        participant.wait().expect("Failed to wait on participant");
    }
    coordinator.report_status();
}

///
/// pub fn run_client(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new client
/// 3. Starts the client protocol
///
fn run_client(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // TODO
    // Connect to the coordinator to get tx/rx

    let (tx, rx) = connect_to_coordinator(opts);

    // Constructs a new client
    let num_requests = opts.num_requests; // Assuming the options have a num_requests field

    let mut client = Client::new(
        format!("client_{}",opts.num),
        Arc::clone(&running),
        tx,
        rx,
        opts.num_requests
    );

    // Starts the client protocol
    client.protocol(num_requests);
}

///
/// pub fn run_participant(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new participant
/// 3. Starts the participant protocol
///
fn run_participant(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_log_path = format!("{}//{}.log", opts.log_path, format!("participant_{}",opts.num));
    let (tx, rx) = connect_to_coordinator(opts);

    // TODO
    // Constructs a new participant
    let mut participant = Participant::new(
        format!("participant_{}",opts.num), 
        participant_log_path,
        running.clone(), 
        opts.send_success_probability,
        opts.operation_success_probability,
        tx, 
        rx);
    // Starts the participant protocol
    participant.protocol();

}

fn main() {
    // Parse CLI arguments
    let mut opts = tpcoptions::TPCOptions::new();
    // Set-up logging and create OpLog path if necessary
    stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!("Failed to create log_path: \"{:?}\". Error \"{:?}\"", opts.log_path, e),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    ctrlc::set_handler(move || {
        println!("Ctrl+C signal received, setting running flag to false.");
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("\n");
        }
    }).expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&mut opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(opts.num_clients, opts.num_requests, opts.num_participants, &opts.log_path),
        _ => panic!("Unknown mode"),
    }
}
