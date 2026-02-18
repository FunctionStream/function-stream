// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod repl;

use clap::Parser;
use repl::Repl;
use std::process;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Parser, Debug)]
#[command(name = "function-stream-cli")]
#[command(about = "Interactive SQL CLI for Function Stream", long_about = None)]
#[command(disable_help_flag = true)]
struct Args {
    #[arg(short = 'h', long = "host", default_value = "127.0.0.1")]
    host: String,

    #[arg(short = 'p', long = "port", default_value = "8080")]
    port: u16,

    #[arg(long = "help", action = clap::ArgAction::Help)]
    help: Option<bool>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let repl = Arc::new(Mutex::new(Repl::new(args.host.clone(), args.port)));

    if let Err(e) = Repl::run_async(repl).await {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
