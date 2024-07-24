use pulsar::TaskLauncher;

mod redis_module;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Parse cli
  let options = pulsar::cli::parse_from_args();

  let mut modules = pulsar::modules();
  modules.push(Box::new(redis_module::module()) as Box<dyn TaskLauncher>);

  match pulsar::run_pulsar_exec(&options, modules).await {
    Ok(_) => std::process::exit(0),
    Err(e) => {
      pulsar::cli::report_error(&e);
      std::process::exit(1);
    }
  }
}