#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include "base/types.h"

#if USE_PARQUET

#include <arrow/io/interfaces.h>
#include <parquet/file_reader.h>

namespace ErrorCodes
{
extern const int CANNOT_ALLOCATE_MEMORY;
extern const int INCORRECT_DATA;
}

namespace parquet
{
class FileMetaData;
}

namespace arrow::io
{
class RandomAccessFile;
}

namespace DB
{

/// Name of the automatically added timestamp column
constexpr static auto TIME_COLUMN_NAME = "time";
/// Name of the automatically added JSON-like object column containing all parquet data
constexpr static auto PARSED_COLUMN_NAME = "data";

/**
 * @brief Input format for reading Parquet files with flexible schema handling
 * 
 * This format extends the basic Parquet reading capabilities by:
 * 1. Adding a timestamp column tracking when each row was read
 * 2. Providing a JSON-like object column containing all parquet data
 * 3. Supporting selective materialization of columns
 * 4. Enabling flexible column access patterns
 */
class ParquetFlexInputFormat : public IInputFormat
{
public:
    /**
     * @brief Construct a new ParquetFlexInputFormat
     * 
     * @param buf - Input buffer containing the parquet data
     * @param header - Expected output schema
     * @param format_settings - Format-specific settings
     * @param max_decoding_threads - Maximum number of threads for decoding
     * @param max_io_threads - Maximum number of threads for I/O operations
     * @param min_bytes_for_seek - Minimum bytes required for seeking
     */
    ParquetFlexInputFormat(
        ReadBuffer & buf,
        const Block & header,
        const FormatSettings & format_settings,
        size_t max_decoding_threads,
        size_t max_io_threads,
        size_t min_bytes_for_seek);

    /// Generate the next chunk of data
    Chunk generate() override;

    /// Get the name of this input format
    String getName() const override { return "ParquetFlexInputFormat"; }

    /// Set key condition for filtering data
    void setKeyCondition(const std::shared_ptr<const KeyCondition> & condition) override;

    /// Set key condition using an ActionsDAG for filtering
    void setKeyCondition(const std::optional<ActionsDAG> & filter_actions_dag, ContextPtr context) override;

protected:
    /// Implementation of setKeyCondition that handles both overloads
    void setKeyConditionImpl(const std::optional<ActionsDAG> & filter_actions_dag, ContextPtr context, const Block & keys);

private:
    /// Not implemented - this format uses generate() instead
    [[noreturn]] Chunk read() override;

    /// Underlying parquet input format
    std::shared_ptr<IInputFormat> source_parquet_format;
    Strings data_column_names;

    FormatSettings settings;        
};

/**
 * @brief Schema reader for ParquetFlex format
 * 
 * This class is responsible for inferring the schema of ParquetFlex files.
 * It adds the standard time and data columns, plus any materialized columns
 * specified in the format settings.
 */
class ParquetFlexSchemaReader : public ISchemaReader
{
public:
    /**
     * @brief Construct a new ParquetFlexSchemaReader
     * 
     * @param buf - Input buffer containing the parquet data
     * @param set - Format settings
     */
    ParquetFlexSchemaReader(ReadBuffer & buf, const FormatSettings & set);

    /// Read and return the schema of the parquet file
    NamesAndTypesList readSchema() override;

private:
    FormatSettings settings;
};

#endif

/**
 * @brief Register the ParquetFlex input format with the format factory
 * 
 * @param factory - Format factory to register with
 */
void registerInputFormatParquetFlex(FormatFactory & factory);
}
