use clap::{App, Arg, SubCommand};
use kvs::KvStore;
use std::env::args;

fn main() {
    if args().len() == 1 {
        panic!();
    }
    let version = env!("CARGO_PKG_VERSION");

    let matches = App::new("kvs")
        .version(version)
        .author("onigbindemy@gmail.com")
        .about("This is a key-value store ")
        .arg(Arg::with_name("version").short("V").long("Version"))
        .subcommand(
            SubCommand::with_name("set")
                .about("adds a new key and value to the db")
                .arg(Arg::with_name("key").index(1).required(true))
                .arg(Arg::with_name("value").index(2).required(true)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("gets a previously saved value for this key")
                .arg(Arg::with_name("key").index(1).required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("removes a previously saved value from the store")
                .arg(Arg::with_name("key").index(1).required(true)),
        )
        .get_matches();

    if matches.is_present("version") {
        println!("{:?}", version);
    }
    match matches.subcommand() {
        ("set", Some(sub_match)) => {
            todo!()
        }

        ("get", Some(sub_match)) => {
            todo!()
        }
        ("rm", Some(sub_match)) => {
            todo!()
        }

        _ => {}
    }
}
