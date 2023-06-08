use bytes::BytesMut;
// Uncomment this block to pass the first stage
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use anyhow::Result;

#[derive(Debug)]
pub enum Command {
    Echo(String),
    Ping(Option<String>),
    Error(String),
    Unknown(String),
}

#[derive(Debug)]
pub enum Value {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<Value>),
}

impl Value {
    pub fn to_command(&self) -> Command {
        println!("to_command");
        println!("self: {:?}", self);
        match self {
            Value::Array(items) => {
                if let Value::BulkString(cmd) = &items[0] {
                    match cmd.to_ascii_lowercase().as_str() {
                        "ping" => {
                            if let Some(Value::BulkString(msg)) = items.get(1) {
                                Command::Ping(Some(msg.to_string()))
                            } else {
                                Command::Ping(None)
                            }
                        }
                        "echo" => {
                            if let Some(Value::BulkString(msg)) = items.get(1) {
                                Command::Echo(msg.to_string())
                            } else {
                                Command::Error("invalid command, need message".to_string())
                            }
                        }
                        _ => Command::Unknown("unknown".to_string()),
                    }
                } else {
                    Command::Unknown("unknown".to_string())
                }
            }
            _ => Command::Unknown("unknown".to_string()),
        }
    }
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::Error(s) => format!("-{}\r\n", s),
            Value::Integer(i) => format!(":{}\r\n", i),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Value::Array(v) => {
                let mut s = String::new();
                s.push_str(&format!("*{}\r\n", v.len()));
                for value in v {
                    s.push_str(&value.to_string());
                }
                s
            }
        }
    }
}

pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(1024),
        }
    }

    pub async fn read(&mut self) -> Result<Option<Value>> {
        loop {
            let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
            if bytes_read == 0 {
                return Ok(None);
            }

            match parse_command(self.buffer.split())? {
                Some((value, _)) => return Ok(Some(value)),
                None => {}
            }
        }
    }

    pub async fn write(&mut self, value: Value) -> Result<()> {
        self.stream.write_all(value.to_string().as_bytes()).await?;
        Ok(())
    }
}

pub fn parse_command(buf: BytesMut) -> Result<Option<(Value, usize)>> {
    match buf[0] as char {
        '*' => {
            let (arr_len, mut consumed) = match readline(&buf[1..]) {
                Some((line, len)) => {
                    let line = std::str::from_utf8(line)?;
                    let arr_len = line.parse::<usize>()?;

                    (arr_len, len + 1)
                }
                None => return Ok(None),
            };

            let mut arr = Vec::with_capacity(arr_len);

            for _ in 0..arr_len {
                match parse_command(BytesMut::from(&buf[consumed..]))? {
                    Some((value, len)) => {
                        arr.push(value);
                        consumed += len;
                    }
                    None => return Ok(None),
                }
            }

            Ok(Some((Value::Array(arr), consumed)))
        }
        '$' => {
            let (str_len, consumed) = match readline(&buf[1..]) {
                Some((line, len)) => {
                    let line = std::str::from_utf8(line)?;
                    let str_len = line.parse::<usize>()?;

                    (str_len, len + 1)
                }
                None => return Ok(None),
            };

            match consumed + str_len + 2 <= buf.len() {
                true => {
                    let str_buf = &buf[consumed..consumed + str_len];
                    let str_buf = std::str::from_utf8(str_buf)?;
                    let str_buf = str_buf.to_string();

                    Ok(Some((Value::BulkString(str_buf), consumed + str_len + 2)))
                }
                false => Ok(None),
            }
        }
        '+' => match readline(&buf[1..]) {
            Some((line, len)) => {
                let line = std::str::from_utf8(line)?;
                let line = line.to_string();

                Ok(Some((Value::SimpleString(line), len + 1)))
            }
            None => Ok(None),
        },
        _ => Err(anyhow::anyhow!("unknown command")),
    }
}

fn readline(buf: &[u8]) -> Option<(&[u8], usize)> {
    for (i, b) in buf.windows(2).enumerate() {
        if b == b"\r\n".as_slice() {
            return Some((&buf[..i], i + 2));
        }
    }
    None
}

#[tokio::main]
async fn main() {
    println!("Starting server...");

    let listener = match TcpListener::bind("127.0.0.1:6379").await {
        Ok(listener) => listener,
        Err(e) => {
            println!("error binding socket: {}", e);
            return;
        }
    };

    loop {
        let conn = listener.accept().await;
        tokio::spawn(async move {
            match conn {
                Ok((stream, _)) => {
                    println!("new client!");
                    if let Err(e) = handle_client(stream).await {
                        println!("error: {}", e);
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
    }
}

pub async fn handle_client(stream: TcpStream) -> Result<()> {
    let mut conn = Connection::new(stream);

    loop {
        match conn.read().await {
            Err(e) => {
                println!("error: {}", e);
                return Ok(());
            }
            Ok(None) => {
                println!("client disconnected");
                return Ok(());
            }
            Ok(Some(value)) => {
                let resp = match value.to_command() {
                    Command::Echo(s) => {
                        println!("echo");
                        Value::SimpleString(s)
                    }
                    Command::Ping(s) => {
                        println!("ping");
                        Value::SimpleString(s.or_else(|| Some("PONG".to_string())).unwrap())
                    }
                    Command::Unknown(s) => {
                        println!("unknown: {}", s);
                        Value::Error("ERR unknown command".to_string())
                    }
                    Command::Error(s) => {
                        println!("error: {}", s);
                        Value::Error(s)
                    }
                };
                conn.write(resp).await?;
            }
        }
    }
}
