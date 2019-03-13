use std::env;
use std::time::Duration;

use zookeeper::{WatchedEvent, Watcher, ZooKeeper};
use zookeeper::Acl;
use zookeeper::CreateMode;
use zookeeper::Stat;

struct ConnWatcher {}

impl Watcher for ConnWatcher {
    fn handle(&self, we: WatchedEvent) {
        println!("The watcher was called: {:?}", we);
    }
}

// FIXME: what about user/pass and ssl (add_auth)?

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
            ("rm", 4) => remove_node(conn_str, &args[3]),
            ("get", 4) => get_node_data(conn_str, &args[3]),
            // FIXME: support export operation
            ("export", 5) => {
                panic!("The export operation is not yet supported.")
            }
            _ => show_help()
        }
    }
}

fn with_zk(conn_str: &str, fx: &Fn(ZooKeeper)) {
    match ZooKeeper::connect(conn_str, Duration::new(30000, 0), ConnWatcher {}) {
        Ok(zk) => fx(zk),
        Err(e) => panic!("Unable to connect to {}: {:?}", conn_str, e)
    }
}

fn list_node(conn_str: &str, node_path: &str) {
    println!("{}", node_path);

    with_zk(&conn_str, &|zk: ZooKeeper| {
        for child in zk.get_children(&node_path, false).unwrap() {
            println!(" - {:?}", child);
        }
        zk.close().unwrap();
    });
}

fn remove_node(conn_str: &str, node_path: &str) {
    with_zk(&conn_str, &|zk: ZooKeeper| {
        match zk.delete(&node_path, None) {
            Ok(_) => println!("(Removed) {}", &node_path),
            Err(_) => panic!("Unable to delete node ({}) - does it have children?", &node_path)
        }
        zk.close().unwrap();
    });
}

// FIXME: see if I can refactor away some duplication
fn get_node_data(conn_str: &str, node_path: &str) {
    match ZooKeeper::connect(conn_str, Duration::new(30000, 0), ConnWatcher {}) {
        Ok(zk) => {
            let (data, stat) = zk.get_data(node_path, false).unwrap();
            println!("{} = {:?}", node_path, String::from_utf8(data).unwrap());
            display_stat(&stat);

            zk.close().unwrap();
        }
        Err(e) => panic!("Unable to connect to {}: {:?}", conn_str, e)
    }
}

// FIXME: see if I can refactor away some duplication
fn set_node_data(conn_str: &str, node_path: &str, node_value: &str) {
    match ZooKeeper::connect(conn_str, Duration::new(30000, 0), ConnWatcher {}) {
        Ok(zk) => {
            match zk.exists(node_path, false) {
                Ok(opt) => {
                    match opt {
                        Some(_) => {
                            let set_stat = zk.set_data(node_path, node_value.as_bytes().to_vec(), None).unwrap();
                            println!("(Updated) {} = {}", node_path, node_value);
                            display_stat(&set_stat);
                        }
                        None => {
                            match zk.create(node_path, node_value.as_bytes().to_vec(), Acl::open_unsafe().clone(), CreateMode::Persistent) {
                                Ok(res) => {
                                    println!("(Created) {} = {}", node_path, node_value);
                                }
                                Err(c_e) => panic!("Unable to create node {}: {}", node_path, c_e)
                            }
                        }
                    }
                }
                Err(err) => panic!("Something bad has happened: {:?}", err)
            }

            zk.close().unwrap();
        }
        Err(e) => panic!("Unable to connect to {}: {:?}", conn_str, e)
    }
}

fn display_stat(stat: &Stat) {
    println!("    czxid:           {}", stat.czxid);
    println!("    mzxid:           {}", stat.mzxid);
    println!("    ctime:           {}", stat.ctime);
    println!("    mtime:           {}", stat.mtime);
    println!("    version:         {}", stat.version);
    println!("    cversion:        {}", stat.cversion);
    println!("    aversion:        {}", stat.aversion);
    println!("    ephemeral_owner: {}", stat.ephemeral_owner);
    println!("    data_length:     {}", stat.data_length);
    println!("    num_children:    {}", stat.num_children);
    println!("    pzxid:           {}", stat.pzxid);
}

fn show_help() {
    println!("ZooKeeper Client
    zk <conn-str> ls <node-path>              - lists the children of node-path
    zk <conn-str> set <node-path> <value>     - sets the value of <node-path> to <value>
    zk <conn-str> get <node-path>             - gets the value of <node-path>
    zk <conn-str> rm <node-path>              - removes the <node-path> (not recursive)
    zk <conn-str> export <node-path> <file>  - exports the node-path data to <file>
    ");
}