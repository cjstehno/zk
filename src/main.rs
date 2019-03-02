extern crate clap;
extern crate fern;
#[macro_use]
extern crate log;

use std::time::Duration;

use clap::{App, Arg, SubCommand};
use zookeeper::{WatchedEvent, Watcher, ZooKeeper};
use zookeeper::Acl;
use zookeeper::CreateMode;

struct ConnWatcher {}

impl Watcher for ConnWatcher {
    fn handle(&self, we: WatchedEvent) {
        info!("The watcher was called: {:?}", we);
    }
}

fn main() {
    let args = App::new("Zookeeper Client")
        .version("0.0.1")
        .author("Christopher J. Stehno <chris@stehno.com>")
        .about("Performs client operations on a Zookeeper server.")
        .arg(Arg::with_name("verbose").long("verbose").short("v").multiple(true).help("Turns on verbose operation logging information."))
        .arg(Arg::with_name("server").long("server").short("s").value_name("SERVER").help("The server connection string.").required(true).takes_value(true))

        .subcommand(SubCommand::with_name("interactive").about("Starts the client in interactive mode."))

        .subcommand(SubCommand::with_name("ls")
            .about("Lists the children of the specified node.")
            .arg(Arg::with_name("node").long("node").short("n").value_name("NODE").required(true).takes_value(true))
        )

        .subcommand(SubCommand::with_name("set")
            .about("Sets the content of a node.")
            .arg(Arg::with_name("node").long("node").short("n").value_name("NODE").required(true).takes_value(true))
            .arg(Arg::with_name("data").long("data").short("d").value_name("VALUE").required(true).takes_value(true))
        )

        .subcommand(SubCommand::with_name("get")
            .about("Gets the content of a node.")
            .arg(Arg::with_name("node").long("node").short("n").value_name("NODE").required(true).takes_value(true))
        )

        // TODO: export

        .get_matches();

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!("[{}] {}", record.level(), message))
        })
        .level(match args.occurrences_of("verbose") {
            0 => log::LevelFilter::Off,
            1 => log::LevelFilter::Info,
            2 => log::LevelFilter::Debug,
            _ => log::LevelFilter::Trace
        })
        .chain(std::io::stdout())
        .apply()
        .unwrap();


    let conn_str = args.value_of("server").unwrap();

    if let Some(matches) = args.subcommand_matches("interactive") {
        // TODO: interactive support
        info!("Interactive connection to {}", conn_str);
    }

    if let Some(matches) = args.subcommand_matches("ls") {
        let node = matches.value_of("node").unwrap();
        info!("Listing node ({})", node);

        match ZooKeeper::connect(conn_str, Duration::new(30000, 0), ConnWatcher {}) {
            Ok(zk) => {
                let children = zk.get_children(node, false).unwrap();
                for child in children {
                    println!("{:?}", child);
                }
                zk.close().unwrap();
            }
            Err(e) => panic!("Unable to connect to {}: {:?}", conn_str, e)
        }
    }

    if let Some(matches) = args.subcommand_matches("get") {
        let node = matches.value_of("node").unwrap();
        info!("Getting node ({})", node);

        match ZooKeeper::connect(conn_str, Duration::new(30000, 0), ConnWatcher {}) {
            Ok(zk) => {
                let (data, _stat) = zk.get_data(node, false).unwrap();
                println!("{:?}", String::from_utf8(data).unwrap());
                zk.close().unwrap();
            }
            Err(e) => panic!("Unable to connect to {}: {:?}", conn_str, e)
        }
    }

    if let Some(matches) = args.subcommand_matches("set") {
        let node = matches.value_of("node").unwrap();
        let value = matches.value_of("data").unwrap();
        info!("Setting node ({}): {}", node, value);

        match ZooKeeper::connect(conn_str, Duration::new(30000, 0), ConnWatcher {}) {
            Ok(zk) => {
                match zk.exists(node, false) {
                    Ok(opt) => {
                        match opt {
                            Some(_) => {
                                let set_stat = zk.set_data(node, value.as_bytes().to_vec(), None).unwrap();
                                println!("Updated: {:?}", set_stat);
                            }
                            None => {
                                match zk.create(node, value.as_bytes().to_vec(), Acl::open_unsafe().clone(), CreateMode::Persistent) {
                                    Ok(res) => {
                                        println!("Created: {}", res);
                                    }
                                    Err(c_e) => panic!("Unable to create node {}: {}", node, c_e)
                                }
                            }
                        }
                    }
                    Err(err) => {
                        panic!("Something bad has happened: {:?}", err);
                    }
                }

                zk.close().unwrap();
            }
            Err(e) => panic!("Unable to connect to {}: {:?}", conn_str, e)
        }
    }
}
