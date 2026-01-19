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

#[derive(Parser, Debug)]
#[command(name = "function-stream-cli")]
#[command(about = "Interactive SQL CLI for Function Stream", long_about = None)]
struct Args {
    #[arg(short = 'i', long = "ip", default_value = "127.0.0.1")]
    ip: String,

    #[arg(short = 'P', long, default_value = "8080")]
    port: u16,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut repl = Repl::new(args.ip.clone(), args.port);

    if let Err(e) = repl.run_async().await {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
