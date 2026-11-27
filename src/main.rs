use std::process::exit;

fn main() {
    let mut args = std::env::args().skip(1);
    let cmd = args.next();
    let result = match cmd.as_deref() {
        Some("node") => quorum::server::serve(match quorum::server::parse_args(args) {
            Ok(cfg) => cfg,
            Err(err) => {
                eprintln!("{err}");
                exit(2);
            }
        }),
        Some("bench") => quorum::bench::run(),
        Some("--help") | Some("-h") | None => {
            eprintln!(
                "quorum node --id N --data DIR --raft ADDR --http ADDR --advertise URL --peers ID=ADDR,... --peer-http ID=URL,...\nquorum bench"
            );
            exit(if cmd.is_none() { 2 } else { 0 });
        }
        Some(other) => {
            eprintln!("unknown command {other}");
            exit(2);
        }
    };
    if let Err(err) = result {
        eprintln!("{err}");
        exit(1);
    }
}
