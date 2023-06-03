// Uncomment this block to pass the first stage
use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpListener;
use std::net::TcpStream;

use anyhow::Result;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("new client!");
                if let Err(e) = handle_client(BufReader::new(stream)) {
                    println!("error: {}", e);
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

pub fn handle_client(mut stream: BufReader<TcpStream>) -> Result<()> {
    let mut buf = String::with_capacity(1024);

    stream.read_line(&mut buf)?;
    stream.read_line(&mut buf)?;
    stream.read_line(&mut buf)?;

    println!("received: {}", buf);

    stream.get_mut().write_all(b"+PONG\r\n")?;

    Ok(())
}

