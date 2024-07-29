use std::sync:: { Arc };
// use anyhow::Error;
use pulsar_core::pdk::{
  CleanExit, ConfigError, Event,
  ModuleConfig,
  ModuleContext,
  ModuleError,
  PulsarModule,
  Version,
  ShutdownSignal
};
use pulsar_core::event::{ Threat };
// use redis::Client;    
use redis::{
  Client,
  // FromRedisValue,
  // from_redis_value,
  JsonCommands,
  // ErrorKind,
  // RedisError,
  // RedisResult,
  // ToRedisArgs,
  // Value //, RedisError, ToRedisArgs,
};
use serde_json::json;

  // struct MyJson(String);

  // fn convert<T>(v: &Value) -> RedisValue<T>
  // where
  //     T: FromRedisValue + Deserialize<'static>,
  // {
  //     let json_str: String = from_redis_value(v)?;

  //     let result: Self = match serde_json::from_str::<T>(&json_str) {
  //         Ok(v) => v,
  //         Err(_err) => return Err((ErrorKind::TypeError, "Parse to JSON Failed").into()),
  //     };

  //     Ok(result)
  // }

  // impl FromRedisValue for MyJson {
  //   fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
  //       convert(v)
  //   }
  // }

const MODULE_NAME: &str = "redis-logger";

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

  // let r_client= Client::open("redis://default:redisroot@127.0.0.1/")?;
  let r_client= Client::open("redis://127.0.0.1/")?;
  let mut r_connection = r_client.get_connection()?;

  // Ping test  
  let value: String = String::from("vvvalue");  
  match r_connection.json_set::<String, String, String, String>("test".to_string(), " newKey".to_string(), &json!({"iteeeem": value}).to_string()) {
    Ok(..) => (),
    Err(err) => println!("Error is: {:?}", err)
  };
  // end Ping test

  // TODO: create the proper indexKey   
  // TODO: check events key array exitence and proceed  

  loop {
    tokio::select! {
      event = receiver.recv() => {
        let event: Arc<Event> = event?;
        
        if config.print_events {
          println!("{event:?}");
        }
        // let ev = Arc::downgrade(&event);

        // println!("here we are");
        let header = event.header(); 

        match header.threat {
          Some(_) => println!("_____threat found"),
          None => (),
        }
        
        let test: bool = event.header().threat.is_some();
        // println!("test is {}", test);

        if let Some(Threat {
          source,
          description,
          extra: _,
        }) = &event.header().threat {  

          println!("{:?}", header);
          println!("Found threat");
  

          let header_string: String = serde_json::to_string(header)?;
          let payload: String = event.payload().to_string();
          let json: String = json!({
            "payload": payload,
            "header": header_string,
            "source": source,
            "description": description,
          }).to_string();

          // let res: RedisResult<bool> = r_connection.json_set("myEventKey", "cP", &"{event:?}");
          // println!("Written cb: {res:?}");   
          match r_connection.json_arr_append::<String, String, String, Vec<i64> >("threats  ".to_string(), "$".to_string(), &json) {
            Ok(_) => (),
            Err(err) => println!("Error occoured: {err:?}"),
          }

        } else {
          let source = &header.source;

          let header_string: String = serde_json::to_string(header)?;
          let payload: String = event.payload().to_string();

          let json: String = json!({
            "payload": payload,
            "header": header_string,
            "source": source,
          }).to_string();
          
          match r_connection.json_arr_append::<String, String, String, Vec<i64> >("events".to_string(), "$".to_string(), &json) {
            Ok(_) => (),
            Err(err) => println!("Error occoured: {err:?}"),
          }
        }
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
