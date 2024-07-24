use std::{collections::HashMap, sync::Arc};
use pulsar_core::pdk::{
  CleanExit, ConfigError, Event, ModuleConfig, ModuleContext, ModuleError, PulsarModule, Version, ShutdownSignal
};
// use redis::Client;    
use redis::{
  Client, JsonCommands, RedisResult //, RedisError, ToRedisArgs,
};

const MODULE_NAME: &str = "pulsar-redis-logger";

pub fn module() -> PulsarModule {
  PulsarModule::new(
    MODULE_NAME,
    Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
    true,
    module_task,
  )
}

async fn module_task(
  ctx: ModuleContext,
  mut shutdown: ShutdownSignal,
) -> Result<CleanExit, ModuleError> {

  let mut receiver = ctx.get_receiver();

  let rx_config = ctx.get_config();

  let config: RedisModuleConfig = rx_config.read()?;
  let _sender = ctx.get_sender();

  let r_client= Client::open("redis://default:redisroot@127.0.0.1/")?;
  let mut r_conn = r_client.get_connection()?;

  // let value: String = String::from("vvvalue");

  // let result: RedisResult<bool> = r_conn.json_set("myEventKey", "cPath", &value)?;

  // fn set_json_bool<P: ToRedisArgs>(key: P, path: P,   b: bool) -> RedisResult<bool> {
  //   let client = Client::open("redis://127.0.0.1")?;
  //   let connection = client.get_connection()?;

  //   // runs `JSON.SET {key} {path} {b}`
  //   connection.json_set(key, path, b)?
  // }
  // let result = set_json_bool("myKey", "cPath", true);


  // println!("{result:?}");

  
  loop {
    tokio::select! {
      event = receiver.recv() => {
        // println!("Some event recieved");
        if config.print_events {
          println!("Reading events");
        }
        let event: Arc<Event> = event?;
        println!("{event:?}");
        
        let header = event.header();
        let payload = event.payload();
        
        // let mut extra = HashMap::new();
        //extra.insert

        // let res: RedisResult<bool> = r_conn.json_set("myEventKey", "cP", &"{event:?}");
        // println!("Written cb: {res:?}");
      },
      r = shutdown.recv() => {
        println!("Stopping");
        return r
      },
    }
  }

}

#[derive(Clone, Debug, Default)]
struct RedisModuleConfig {
  print_events: bool,
  // read: Client
}

impl TryFrom<&ModuleConfig> for RedisModuleConfig {
  type Error = ConfigError;

  fn try_from(config: &ModuleConfig) -> Result<Self, Self::Error> {
    Ok(RedisModuleConfig {
      print_events: config.with_default("print_events", false)?,
    })
  }
}
