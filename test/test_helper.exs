# Initialize Khepri for testing
store_id = :khepri_store_test
ra_system = :default

# Start required applications in order
{:ok, _} = :application.ensure_all_started(:crypto)
{:ok, _} = :application.ensure_all_started(:asn1)
{:ok, _} = :application.ensure_all_started(:public_key)
{:ok, _} = :application.ensure_all_started(:ssl)
{:ok, _} = :application.ensure_all_started(:gen_batch_server)
{:ok, _} = :application.ensure_all_started(:aten)
{:ok, _} = :application.ensure_all_started(:ra)
{:ok, _} = :application.ensure_all_started(:seshat)
{:ok, _} = :application.ensure_all_started(:khepri)

# Start default Ra system
{:ok, _} = :ra.start_in(~c"test_data")

# Give Ra system time to start
:timer.sleep(500)

# Start Khepri with a test store using the Ra system name
case :khepri.start(ra_system, store_id) do
  {:ok, _} -> :ok
  {:error, {:already_started, _}} -> :ok
  {:error, reason} -> raise "Failed to start Khepri: #{inspect(reason)}"
end

# Wait for Khepri to be fully started
:timer.sleep(500)

# Configure the application to use the test store
Application.put_env(:rabbitmq_message_deduplication, :khepri_store_id, store_id)

# Force rabbit_khepri to use Khepri backend in tests
# Use persistent_term directly since the function may not be exported
:persistent_term.put({:rabbit_khepri, :forced_metadata_store}, :khepri)

ExUnit.start()
