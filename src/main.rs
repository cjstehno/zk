use std::env;
use std::time::Duration;

use zookeeper::{WatchedEvent, Watcher, ZooKeeper};
use zookeeper::Acl;
use zookeeper::CreateMode;

struct ConnWatcher {}

impl Watcher for ConnWatcher {
    fn handle(&self, we: WatchedEvent) {
        println!("The watcher was called: {:?}", we);
    }
}

/*
zk <conn-str> ls /
zk <conn-str> set / <value>
zk <conn-str> get /
zk <conn-str> export / <file>
*/

fn main() {
    let args: Vec<String> = env::args().collect();
    let arg_count = args.len();

    if arg_count < 3 {
        show_help()
    } else {
        let conn_str = &args[1];
        let operation = &args[2];

        match (operation.as_str(), arg_count) {
            ("ls", 4) => list_node(conn_str, &args[3]),
            ("set", 5) => set_node_data(conn_str, &args[3], &args[4]),
            ("get", 4) => get_node_data(conn_str, &args[3]),
            ("export", 5) => {
                panic!("The export operation is not yet supported.")
            }
            _ => show_help()
        }
    }
}

fn list_node(conn_str: &str, node_path: &str) {
    println!("{}", node_path);

    match ZooKeeper::connect(conn_str, Duration::new(30000, 0), ConnWatcher {}) {
        Ok(zk) => {
            let children = zk.get_children(node_path, false).unwrap();
            for child in children {
                println!(" - {:?}", child);
            }
            zk.close().unwrap();
        }
        Err(e) => panic!("Unable to connect to {}: {:?}", conn_str, e)
    }
}

fn get_node_data(conn_str: &str, node_path: &str) {
    match ZooKeeper::connect(conn_str, Duration::new(30000, 0), ConnWatcher {}) {
        Ok(zk) => {
            let (data, _stat) = zk.get_data(node_path, false).unwrap();
            println!("{} = {:?}", node_path, String::from_utf8(data).unwrap());
            zk.close().unwrap();
        }
        Err(e) => panic!("Unable to connect to {}: {:?}", conn_str, e)
    }
}

fn set_node_data(conn_str: &str, node_path: &str, node_value: &str){
    match ZooKeeper::connect(conn_str, Duration::new(30000, 0), ConnWatcher {}) {
        Ok(zk) => {
            match zk.exists(node_path, false) {
                Ok(opt) => {
                    match opt {
                        Some(_) => {
                            let set_stat = zk.set_data(node_path, node_value.as_bytes().to_vec(), None).unwrap();
                            println!("Updated: {}", node_path);
                        }
                        None => {
                            match zk.create(node_path, node_value.as_bytes().to_vec(), Acl::open_unsafe().clone(), CreateMode::Persistent) {
                                Ok(res) => {
                                    println!("Created: {}", res);
                                }
                                Err(c_e) => panic!("Unable to create node {}: {}", node_path, c_e)
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

fn show_help() {
    println!("ZooKeeper Client
    zk <conn-str> ls /
    zk <conn-str> set / <value>
    zk <conn-str> get /
    zk <conn-str> export / <file>
    ");
}