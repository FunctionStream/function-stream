mod repl;

use clap::Parser;
use repl::Repl;
use std::process;

#[derive(Parser, Debug)]
#[command(name = "function-stream-cli")]
#[command(about = "Interactive SQL CLI for Function Stream", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "admin")]
    username: String,

    #[arg(short = 'w', long, default_value = "admin")]
    password: String,

    #[arg(short = 'i', long = "ip", default_value = "127.0.0.1")]
    ip: String,

    #[arg(short = 'P', long, default_value = "8080")]
    port: u16,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut repl = Repl::new(args.ip.clone(), args.port);

    match repl.authenticate(args.username.clone(), args.password).await {
        Ok(true) => {
            if let Err(e) = repl.run_async().await {
                eprintln!("Error: {}", e);
                process::exit(1);
            }
        }
        Ok(false) => {
            eprintln!("Authentication failed: Invalid username or password");
            process::exit(1);
        }
        Err(e) => {
            eprintln!("Authentication error: {}", e);
            process::exit(1);
        }
    }
}
