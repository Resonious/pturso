mod port;

use port::{new_port, ResponseTx};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use pmrpc::define_requests;

#[derive(Debug, Serialize, Deserialize)]
pub struct Select {
    pub query: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RowsResult {
    Ok(Vec<serde_json::Value>),
    Error(String),
}

define_requests! {
    (Serialize, Deserialize)

    Select => RowsResult,
}

#[tokio::main]
async fn main() {
    let (mut reader, writer, response_tx, response_rx) = new_port(100);

    // Spawn the writer task
    let writer_handle = tokio::spawn(async move {
        if let Err(e) = writer.run(response_rx).await {
            eprintln!("Writer error: {}", e);
        }
    });

    // Read requests and spawn handler tasks
    loop {
        match reader.read_request().await {
            Ok(Some(request)) => {
                tokio::spawn(handle_request(request, response_tx.clone()));
            }
            Ok(None) => {
                // EOF - Erlang closed the port
                break;
            }
            Err(e) => {
                eprintln!("Failed to read request: {}", e);
                break;
            }
        }
    }

    // Drop the sender so the writer task can finish
    drop(response_tx);
    let _ = writer_handle.await;
}

async fn handle_request(request: port::Request, tx: ResponseTx) {
    // TODO: Implement request handling logic here

    // libsql::Builder::new_remote_replica();

    // For now, echo the data back
    let response = port::Response {
        id: request.id,
        data: request.data,
    };

    if let Err(e) = tx.send(response).await {
        eprintln!("Failed to send response: {}", e);
    }
}
