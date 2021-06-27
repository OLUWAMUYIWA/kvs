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
    let mut store = KvStore::new();

    if matches.is_present("version") {
        println!("{:?}", version);
    }
    match matches.subcommand() {
        ("set", Some(sub_match)) => {
            let key = sub_match.value_of("key").unwrap();
            let value = sub_match.value_of("value").unwrap();
            store.set(key.to_owned(), value.to_owned());
            panic!("unimplemented");
        }

        ("get", Some(sub_match)) => {
            let key = sub_match.value_of("key").unwrap();
            store.get(key.to_owned());
            panic!("unimplemented");
            //eprintln!("unimmplemented");
        }
        ("rm", Some(sub_match)) => {
            let key = sub_match.value_of("key").unwrap();
            store.remove(key.to_owned());
            panic!("unimplemented")
        }

        _ => {}
    }
}
