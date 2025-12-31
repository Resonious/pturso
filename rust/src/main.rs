mod port;

use std::marker::PhantomData;

use port::{new_port, ResponseTx};
use wincode::{SchemaRead, SchemaWrite};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use pmrpc::define_requests;

#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct Crap {
    reason: String,
}
#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct BadRequest {
    reason: String,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct Select {
    pub db: String,
    pub query: String,
    pub params: Vec<Value>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
pub enum RowsResult {
    Ok(Vec<Value>),
    Error(String),
}

#[derive(Clone, Debug, PartialEq, SchemaRead, SchemaWrite)]
pub enum Value {
    Null,
    Integer(i64),  // Should correspond to Erlang Int
    Real(f64),     // Should correspond to Erlang Float
    Text(String),  // Should correspond to Erlang <<"hi"/utf8> bit string
    Blob(Vec<u8>), // Should correspond to Erlang bit string
}

impl Into<turso::Value> for Value {
    fn into(self) -> turso::Value {
        match self {
            Value::Null => turso::Value::Null,
            Value::Integer(x) => turso::Value::Integer(x),
            Value::Real(x) => turso::Value::Real(x),
            Value::Text(x) => turso::Value::Text(x),
            Value::Blob(x) => turso::Value::Blob(x),
        }
    }
}

impl From<turso::Value> for Value {
    fn from(value: turso::Value) -> Value {
        match value {
            turso::Value::Null => Value::Null,
            turso::Value::Integer(x) => Value::Integer(x),
            turso::Value::Real(x) => Value::Real(x),
            turso::Value::Text(x) => Value::Text(x),
            turso::Value::Blob(x) => Value::Blob(x),
        }
    }
}

// NOTE: this produces enums that look like this:
//
// #[derive(SchemaRead, SchemaWrite)]
// enum Requests {
//     Select(Select),
//     ...
// }
//
// #[derive(SchemaRead, SchemaWrite)]
// enum Responses {
//     RowsResult(RowsResult),
//     ...
// }
define_requests! {
    (SchemaRead, SchemaWrite)

    Crap => BadRequest,
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
                tokio::spawn(process_request(request, response_tx.clone()));
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

/// In charge of deserializing the request and serializing the response.
async fn process_request(request: port::Request, tx: ResponseTx) {
    let req: Result<Requests, _> = wincode::deserialize(&request.data);
    let req = match req {
        Ok(req) => req,
        Err(e) => Crap { reason: e.to_string() }.into(),
    };

    let resp = handle_request(req).await;
    let resp_data = wincode::serialize(&resp)
        .unwrap_or_else(|e| {
            eprintln!("SERIALIZE ERROR: {e:?}");
            Vec::new()
        });

    let response = port::Response {
        id: request.id,
        data: resp_data,
    };

    if let Err(e) = tx.send(response).await {
        eprintln!("Failed to send response: {}", e);
    }
}

/// In charge of actually performing the requested operation.
async fn handle_request(req: Requests) -> Responses {
    match req {
        Requests::Crap(crap, p) => {
            let resp = BadRequest { reason: crap.reason };
            response_enum(p, resp)
        }

        _ => todo!()
    }
}

pub fn response_enum<Req, Resp>(_req: PhantomData<Req>, resp: Resp) -> Responses
where
    Req: RespondsWith<Resp> + Send,
    Resp: Send,
{
    Req::resp_enum(resp)
}
