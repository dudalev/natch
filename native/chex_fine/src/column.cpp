#include <fine.hpp>
#include <clickhouse/client.h>
#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/date.h>
#include <string>
#include <memory>
#include <stdexcept>

using namespace clickhouse;

// Wrapper to hold shared_ptr<Column> since FINE uses ResourcePtr
struct ColumnResource {
  std::shared_ptr<Column> ptr;

  ColumnResource(std::shared_ptr<Column> p) : ptr(p) {}
};

// Declare ColumnResource as a FINE resource
FINE_RESOURCE(ColumnResource);

// Create a column by type name
// Uses clickhouse-cpp's CreateColumnByType for dynamic type creation
fine::ResourcePtr<ColumnResource> column_create(
    ErlNifEnv *env,
    std::string type_name) {
  try {
    auto col = CreateColumnByType(type_name);
    if (!col) {
      throw std::runtime_error("Failed to create column of type: " + type_name);
    }
    return fine::make_resource<ColumnResource>(col);
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Column creation failed: ") + e.what());
  }
}
FINE_NIF(column_create, 0);

// Append UInt64 value
// DEPRECATED: Use column_uint64_append_bulk for better performance
fine::Atom column_uint64_append(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    uint64_t value) {
  try {
    auto typed = std::static_pointer_cast<ColumnUInt64>(col_res->ptr);
    typed->Append(value);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("UInt64 append failed: ") + e.what());
  }
}
FINE_NIF(column_uint64_append, 0);

// Append Int64 value
// DEPRECATED: Use column_int64_append_bulk for better performance
fine::Atom column_int64_append(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    int64_t value) {
  try {
    auto typed = std::static_pointer_cast<ColumnInt64>(col_res->ptr);
    typed->Append(value);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Int64 append failed: ") + e.what());
  }
}
FINE_NIF(column_int64_append, 0);

// Append String value
// DEPRECATED: Use column_string_append_bulk for better performance
fine::Atom column_string_append(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::string value) {
  try {
    auto typed = std::static_pointer_cast<ColumnString>(col_res->ptr);
    typed->Append(value);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("String append failed: ") + e.what());
  }
}
FINE_NIF(column_string_append, 0);

// Append Float64 value
// DEPRECATED: Use column_float64_append_bulk for better performance
fine::Atom column_float64_append(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    double value) {
  try {
    auto typed = std::static_pointer_cast<ColumnFloat64>(col_res->ptr);
    typed->Append(value);
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Float64 append failed: ") + e.what());
  }
}
FINE_NIF(column_float64_append, 0);

// Append DateTime value (Unix timestamp as uint64)
// DEPRECATED: Use column_datetime_append_bulk for better performance
fine::Atom column_datetime_append(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    uint64_t timestamp) {
  try {
    auto typed = std::static_pointer_cast<ColumnDateTime>(col_res->ptr);
    typed->Append(static_cast<time_t>(timestamp));
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("DateTime append failed: ") + e.what());
  }
}
FINE_NIF(column_datetime_append, 0);

// Get column size (number of rows)
uint64_t column_size(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res) {
  return col_res->ptr->Size();
}
FINE_NIF(column_size, 0);

//
// BULK APPEND OPERATIONS
// These functions accept vectors of values for efficient bulk insertion
// Reduces NIF boundary crossings from N (one per value) to 1 (one per column)
//

// Bulk append UInt64 values
fine::Atom column_uint64_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<uint64_t> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnUInt64>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(value);
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("UInt64 bulk append failed: ") + e.what());
  }
}
FINE_NIF(column_uint64_append_bulk, 0);

// Bulk append Int64 values
fine::Atom column_int64_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<int64_t> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnInt64>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(value);
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Int64 bulk append failed: ") + e.what());
  }
}
FINE_NIF(column_int64_append_bulk, 0);

// Bulk append String values
fine::Atom column_string_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<std::string> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnString>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(value);
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("String bulk append failed: ") + e.what());
  }
}
FINE_NIF(column_string_append_bulk, 0);

// Bulk append Float64 values
fine::Atom column_float64_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<double> values) {
  try {
    auto typed = std::static_pointer_cast<ColumnFloat64>(col_res->ptr);
    for (const auto& value : values) {
      typed->Append(value);
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Float64 bulk append failed: ") + e.what());
  }
}
FINE_NIF(column_float64_append_bulk, 0);

// Bulk append DateTime values (Unix timestamps as uint64)
fine::Atom column_datetime_append_bulk(
    ErlNifEnv *env,
    fine::ResourcePtr<ColumnResource> col_res,
    std::vector<uint64_t> timestamps) {
  try {
    auto typed = std::static_pointer_cast<ColumnDateTime>(col_res->ptr);
    for (const auto& timestamp : timestamps) {
      typed->Append(static_cast<time_t>(timestamp));
    }
    return fine::Atom("ok");
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("DateTime bulk append failed: ") + e.what());
  }
}
FINE_NIF(column_datetime_append_bulk, 0);
