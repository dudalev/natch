#include <fine.hpp>
#include <clickhouse/client.h>
#include <string>
#include <stdexcept>

using namespace clickhouse;

// Declare Client as a FINE resource
FINE_RESOURCE(Client);

// Helper to handle nullable strings from Elixir (nil becomes empty string)
std::string get_optional_string(const std::string& value) {
  return value;
}

// Create a ClickHouse client with full options
// Args: host, port, database (nil/empty for none), user (nil/empty for none),
//       password (nil/empty for none), compression_enabled
// Note: FINE converts Elixir nil to empty string for string params
fine::ResourcePtr<Client> client_create(
    ErlNifEnv *env,
    std::string host,
    uint64_t port,
    std::string database,
    std::string user,
    std::string password,
    bool compression) {
  try {
    ClientOptions opts;
    opts.SetHost(host);
    opts.SetPort(static_cast<uint16_t>(port));

    if (!database.empty()) {
      opts.SetDefaultDatabase(database);
    }

    if (!user.empty()) {
      opts.SetUser(user);
    }

    if (!password.empty()) {
      opts.SetPassword(password);
    }

    if (compression) {
      opts.SetCompressionMethod(CompressionMethod::LZ4);
    }

    return fine::make_resource<Client>(opts);
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to create client: ") + e.what());
  }
}
FINE_NIF(client_create, 0);

// Simple client creation (for PoC compatibility)
fine::ResourcePtr<Client> create_client(ErlNifEnv *env) {
  return client_create(env, "localhost", 9000, "", "", "", false);
}
FINE_NIF(create_client, 0);

// Ping the ClickHouse server
std::string client_ping(ErlNifEnv *env, fine::ResourcePtr<Client> client) {
  try {
    client->Ping();
    return "pong";
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Ping failed: ") + e.what());
  }
}
FINE_NIF(client_ping, 0);

// Alias for backwards compatibility with PoC
std::string ping(ErlNifEnv *env, fine::ResourcePtr<Client> client) {
  return client_ping(env, client);
}
FINE_NIF(ping, 0);

// Execute a query (DDL/DML without results)
// Returns :ok atom on success
fine::Atom client_execute(
    ErlNifEnv *env,
    fine::ResourcePtr<Client> client,
    std::string sql) {
  try {
    client->Execute(sql);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Execute failed: ") + e.what());
  }
}
FINE_NIF(client_execute, 0);

// Reset connection
// Returns :ok atom on success
fine::Atom client_reset_connection(ErlNifEnv *env, fine::ResourcePtr<Client> client) {
  try {
    client->ResetConnection();
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Reset connection failed: ") + e.what());
  }
}
FINE_NIF(client_reset_connection, 0);

// Initialize the NIF module
FINE_INIT("Elixir.Chex.Native");
