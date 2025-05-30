#include "ParquetFlexInputFormat.h"
#include <algorithm>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeObject.h>
#include <Interpreters/castColumn.h>
#include <boost/algorithm/string.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include <Common/assert_cast.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"
#include "Columns/IColumn.h"
#include "Processors/Formats/Impl/ParquetBlockInputFormat.h"
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Interpreters/ExpressionActions.h>

#if USE_PARQUET

namespace DB
{

// ---- Helper functions ----

/**
 * @brief Reads and converts the Parquet file header to a Block
 * 
 * This function reads the Parquet file metadata and converts its schema to a Block format.
 * It handles the conversion from Parquet/Arrow schema to ClickHouse types, taking into account
 * various format settings for schema inference.
 * 
 * @param buf - Input buffer containing the parquet data
 * @param format_settings - Format settings controlling schema inference behavior
 * @return Block containing the converted schema
 * @throws Exception if schema conversion fails
 */
Block readParquetHeader(ReadBuffer & buf, const FormatSettings & format_settings)
{
    std::atomic<int> is_stopped{0};
    auto arrow_file = asArrowFile(buf, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, true);
    auto metadata = parquet::ReadMetaData(arrow_file);

    std::shared_ptr<arrow::Schema> schema;
    if (::arrow::Status status = (parquet::arrow::FromParquetSchema(metadata->schema(), &schema)); !status.ok())
    {
        throw Exception(
            status.IsOutOfMemory() ? ErrorCodes::CANNOT_ALLOCATE_MEMORY : ErrorCodes::INCORRECT_DATA,
            "Failed to convert Parquet schema to Arrow schema: {}",
            status.ToString());
    }

    return ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *schema,
        metadata->key_value_metadata(),
        "Parquet",
        format_settings.parquet.skip_columns_with_unsupported_types_in_schema_inference,
        format_settings.schema_inference_make_columns_nullable != 0,
        format_settings.parquet.case_insensitive_column_matching,
        format_settings.parquet.allow_geoparquet_parser);
}

/**
 * @brief Qualifies column names based on a template pattern
 * 
 * This function processes a column template string to determine which columns from the header
 * should be included. It supports various patterns:
 * - "*" matches all columns
 * - "*pattern*" matches columns containing the pattern
 * - "pattern*" matches columns starting with the pattern
 * - "*pattern" matches columns ending with the pattern
 * - exact name matches the specific column
 * 
 * @param column_template - Template string defining column patterns (comma separated list of patterns)
 * @param header - Block containing available columns
 * @return List of qualified column names
 */
Strings qualifyingColumns(String column_template, const Block & header)
{
    Strings ret;
    Strings list_elements;
    boost::split(list_elements, column_template, [](char c) { return c == ','; });

    for (auto & element : list_elements)
    {
        boost::trim(element);
        if (element == "*")
        {
            ret.clear();
            for (const auto & column : header)
                ret.push_back(column.name);

            return ret;
        }
        else if (element.starts_with("*") && element.ends_with("*"))
        {
            element = element.substr(1, element.size() - 2);
            if (element.empty())
            { // '**' == '*'
                ret.clear();
                for (const auto & column : header)
                    ret.push_back(column.name);

                return ret;
            }
            else
            {
                for (const auto & column : header)
                {
                    if (column.name.find(element) != std::string::npos && std::find(ret.begin(), ret.end(), column.name) == ret.end())
                    {
                        ret.push_back(column.name);
                    }
                }
            }
        }
        else if (element.ends_with("*"))
        {
            element = element.substr(0, element.size() - 1);
            for (const auto & column : header)
            {
                if (column.name.starts_with(element) && std::find(ret.begin(), ret.end(), column.name) == ret.end())
                {
                    ret.push_back(column.name);
                }
            }
        }
        else if (element.starts_with("*"))
        {
            element = element.substr(1);
            for (const auto & column : header)
            {
                if (column.name.ends_with(element) && std::find(ret.begin(), ret.end(), column.name) == ret.end())
                {
                    ret.push_back(column.name);
                }
            }
        }
        else if (
            std::find_if(header.begin(), header.end(), [&](const auto & column) { return column.name == element; }) != header.end()
            && std::find(ret.begin(), ret.end(), element) == ret.end())
        {
            ret.push_back(element);
        }
    }

    return ret;
}

/**
 * @brief Gets list of columns to materialize from format settings
 * The materialized columns are top-level columns that can be used to apply filters on.
 * 
 * @param settings - Format settings containing materialization configuration
 * @param parquet_header - Block containing parquet schema information
 * @return List of column names to materialize
 */
Strings materializationsFromSettings(const FormatSettings & settings, Block parquet_header)
{
    auto materializations = settings.flex.materialized_fields;
    if (materializations.empty())
        return {};

    return qualifyingColumns(materializations, parquet_header);
}

/**
 * @brief Gets list of data columns based on format settings
 * The data columns are all columns that end up in the JSON-like object column.
 * 
 * @param settings - Format settings containing data field configuration
 * @param parquet_header - Block containing parquet schema information
 * @return List of data column names
 */
Strings dataColumns(const FormatSettings & settings, const Block & parquet_header)
{
    Strings data_column_names;

    auto data_fields = settings.flex.data_fields;
    if (data_fields.empty())
        data_fields = "*";

    return qualifyingColumns(data_fields, parquet_header);
}

/**
 * @brief Merges two lists of column names while preserving order and avoiding duplicates
 * 
 * This function combines two lists of column names into a single list. The resulting list
 * will contain all unique column names from both input lists, with the order preserved
 * from the first list followed by any unique columns from the second list.
 * 
 * @param list1 - First list of column names
 * @param list2 - Second list of column names to merge with the first
 * @return A new list containing all unique column names from both input lists
 */
Strings mergeColumnLists(const Strings & list1, const Strings & list2)
{
    Strings ret = list1;
    for (const auto & column : list2)
    {
        if (std::find(ret.begin(), ret.end(), column) == ret.end())
            ret.push_back(column);
    }

    return ret;
}

/**
 * @brief Registers the ParquetFlex input format with the format factory
 * 
 * This function registers both the input format and schema reader for the ParquetFlex format.
 * It configures the format to support random access and subset of columns.
 * 
 * @param factory - Format factory to register with
 */
void registerInputFormatParquetFlex(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "ParquetFlex",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & read_settings,
           bool is_remote_fs,
           size_t max_download_threads,
           size_t max_parsing_threads) -> std::shared_ptr<IInputFormat>
        {
            size_t min_bytes_for_seek
                = is_remote_fs ? read_settings.remote_read_min_bytes_for_seek : settings.parquet.local_read_min_bytes_for_seek;
            return std::make_shared<ParquetFlexInputFormat>(
                buf, sample, settings, max_parsing_threads, max_download_threads, min_bytes_for_seek);
        });
    factory.markFormatSupportsSubsetOfColumns("ParquetFlex");

    factory.registerSchemaReader(
        "ParquetFlex",
        [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<ParquetFlexSchemaReader>(buf, settings); });
}

// ---- ParquetFlexInputFormat Implementation ----

/**
 * @brief Constructs a new ParquetFlexInputFormat
 * 
 * The constructor reads the parquet header and sets up the underlying parquet input format
 * with the projected columns based on materialization settings.
 *
 * @param buf - Input buffer containing the parquet data
 * @param header - Expected output schema
 * @param format_settings - Format-specific settings
 * @param max_decoding_threads - Maximum number of threads for decoding
 * @param max_io_threads - Maximum number of threads for I/O operations
 * @param min_bytes_for_seek - Minimum bytes required for seeking
 */
ParquetFlexInputFormat::ParquetFlexInputFormat(
    ReadBuffer & buf,
    const Block & header,
    const FormatSettings & format_settings,
    size_t max_decoding_threads,
    size_t max_io_threads,
    size_t min_bytes_for_seek)
    : IInputFormat(header, &buf)
    , settings(format_settings)
{
    Block parquet_header = readParquetHeader(buf, settings);
    data_column_names = dataColumns(settings, parquet_header);
    const auto materializations = materializationsFromSettings(settings, parquet_header);
    const auto projected_column_names = mergeColumnLists(data_column_names, materializations);

    // find all parquet columns, not listed in the materializations property
    for (size_t i = 0; i < parquet_header.columns(); ++i)
    {
        const auto & column = parquet_header.getByPosition(i);
        if (std::find(projected_column_names.begin(), projected_column_names.end(), column.name) == projected_column_names.end())
        {
            parquet_header.erase(i--);
        }
    }

    source_parquet_format = std::make_shared<ParquetBlockInputFormat>(
        buf, parquet_header, format_settings, max_decoding_threads, max_io_threads, min_bytes_for_seek);
}

/**
 * @brief Generates the next chunk of data
 * 
 * This function generates a chunk of data by:
 * 1. Reading data from the underlying parquet format
 * 2. Adding a timestamp column with current time
 * 3. Creating a JSON-like object column containing all parquet data
 * 4. Materializing requested columns
 * 
 * @return Chunk containing the generated data
 */
Chunk ParquetFlexInputFormat::generate()
{
    const Block & header = getPort().getHeader(); // our fixed output column names/types
    Columns columns; // array of our output columns
    columns.reserve(header.columns());

    Chunk parquet_chunk = source_parquet_format->generate();
    auto num_rows = parquet_chunk.getNumRows(); // memorize how many rows are in the chunk (before detaching)
    auto parquet_columns = parquet_chunk.detachColumns(); // take ownership of the columns

    for (const auto & column : header) // iterate over the (output) columns of the InputFormat (fixed columns)
    {
        if (column.name == TIME_COLUMN_NAME)
        {
            // TODO: Real code would map a parquet column of numeric type to DateTime64
            auto column_ptr = column.type->createColumn();
            auto * col = assert_cast<ColumnDecimal<DateTime64> *>(column_ptr.get());

            for (size_t i = 0; i < num_rows; i++)
            {
                auto now = std::chrono::system_clock::now();
                auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
                col->insertValue(ns / 1000);
            }
            columns.push_back(std::move(column_ptr));
        }
        else if (column.name == PARSED_COLUMN_NAME)
        {
            auto column_ptr = column.type->createColumn();
            auto * col = assert_cast<ColumnObject *>(column_ptr.get());
            const auto & parquet_header = source_parquet_format->getPort().getHeader();

            // TODO: Implement more efficient way to move the parquet column into
            //       the ColumnObject directly instead of constructing a temprary
            //       Object.
            Object row_data;

            // For each row, create an object with all parquet columns
            for (size_t row = 0; row < num_rows; ++row)
            {
                // Add each parquet column to the row data
                for (const auto & column_name : data_column_names)
                {
                    const auto & parquet_column_data = parquet_columns[parquet_header.getPositionByName(column_name)];
                    row_data[column_name] = parquet_column_data->operator[](row);
                }

                // Create a Field containing the Object and insert into ColumnObject
                col->insert(row_data);
            }

            /// Create chunk with the ColumnObject
            columns.push_back(std::move(column_ptr));
        }
        else
        {
            const auto & parquet_header = source_parquet_format->getPort().getHeader();

            if (parquet_header.has(column.name))
            {
                const auto parquet_col_header = parquet_header.getByName(column.name);
                const auto parquet_column_pos = parquet_header.getPositionByName(column.name);
                const auto & parquet_column_data = parquet_columns[parquet_column_pos];

                ColumnWithTypeAndName col_with_type{parquet_column_data, parquet_col_header.type, column.name};
                auto dynamic_column = castColumn(col_with_type, column.type);
                columns.push_back(dynamic_column);
            }
            else
            {
                columns.push_back(column.type->createColumnConstWithDefaultValue(num_rows));
            }
        }
    }

    return Chunk(std::move(columns), num_rows);
}

/**
 * @brief Sets a key condition for filtering data
 * 
 * @param condition - Key condition to apply for filtering
 */
void ParquetFlexInputFormat::setKeyCondition(const std::shared_ptr<const KeyCondition> & condition)
{
    if (!condition)
    {
        source_parquet_format->setKeyCondition(nullptr);
        return;
    }

    const auto parquet_header = source_parquet_format->getPort().getHeader();
    const auto flex_key_columns = condition->getKeyColumns();
    std::vector<KeyCondition::RPNElement> new_rpn_list;
    std::map<String, size_t> new_key_columns;
    new_rpn_list.reserve(condition->getRPN().size());

    // Create a new Context with the FormatSettings 
    auto context = Context::createGlobal(nullptr);

    // Map each RPN element to the inner format's column positions
    for (const auto & rpn : condition->getRPN())
    {
        if (rpn.function == KeyCondition::RPNElement::FUNCTION_UNKNOWN)
            continue;

        // Find the column name by its position in the outer format
        auto it = std::find_if(flex_key_columns.begin(), flex_key_columns.end(),
            [&](const auto & pair) { return pair.second == rpn.key_column; });

        if (it != flex_key_columns.end())
        {
            const auto & key_column_name = it->first;
            if (parquet_header.has(key_column_name))
            {
                // Create a new RPN element with the mapped column position
                KeyCondition::RPNElement new_element = rpn;
                new_element.key_column = parquet_header.getPositionByName(key_column_name);
                new_rpn_list.push_back(new_element);

                // Add to key columns map
                new_key_columns[key_column_name] = new_element.key_column;
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, 
                "Column with position {} found in RPN but not in key columns map", 
                rpn.key_column);
        }
    }

    // Create a new ActionsDAG with the mapped RPN elements
    auto dag = std::make_shared<ActionsDAG>();
    std::vector<const ActionsDAG::Node *> stack;

    // Add inputs for each mapped column
    for (const auto & [name, pos] : new_key_columns)
    {
        const auto * node = &dag->addInput(parquet_header.getByPosition(pos));
        stack.push_back(node);
    }

    // Add nodes for each RPN element
    for (const auto & element : new_rpn_list)
    {
        switch (element.function)
        {
            case KeyCondition::RPNElement::FUNCTION_IN_RANGE:
            {
                auto func = FunctionFactory::instance().get("range", context);
                const auto * node = &dag->addFunction(func, {stack.back()}, {});
                stack.back() = node;
                break;
            }
            case KeyCondition::RPNElement::FUNCTION_AND:
            {
                auto func = FunctionFactory::instance().get("and", context);
                const auto * right = stack.back();
                stack.pop_back();
                const auto * left = stack.back();
                stack.back() = &dag->addFunction(func, {left, right}, {});
                break;
            }
            case KeyCondition::RPNElement::FUNCTION_OR:
            {
                auto func = FunctionFactory::instance().get("or", context);
                const auto * right = stack.back();
                stack.pop_back();
                const auto * left = stack.back();
                stack.back() = &dag->addFunction(func, {left, right}, {});
                break;
            }
            case KeyCondition::RPNElement::FUNCTION_NOT:
            {
                auto func = FunctionFactory::instance().get("not", context);
                stack.back() = &dag->addFunction(func, {stack.back()}, {});
                break;
            }
            default:
                break;
        }
    }

    // Create ActionsDAGWithInversionPushDown from the last node on the stack
    ActionsDAGWithInversionPushDown inverted_dag(stack.back(), context);

    // Create a new condition using the standard constructor
    auto new_condition = std::make_shared<KeyCondition>(
        inverted_dag,
        context,
        Names(parquet_header.getNames()),
        std::make_shared<ExpressionActions>(
            ActionsDAG(parquet_header.getColumnsWithTypeAndName()),
            ExpressionActionsSettings()));
    source_parquet_format->setKeyCondition(new_condition);
}

/**
 * @brief Sets a key condition using an ActionsDAG for filtering
 * 
 * @param filter_actions_dag - ActionsDAG defining the filter
 * @param context - Query context
 */
void ParquetFlexInputFormat::setKeyCondition(const std::optional<ActionsDAG> & filter_actions_dag, ContextPtr context)
{
    source_parquet_format->setKeyCondition(filter_actions_dag, context);
}

/**
 * @brief This function is not implemented because it is not needed for this input format.
 * @details The read() function is only used by the inner input format to get the ReadBuffer for 
 *          the generate() calls. As we don't interprete data ourselves here, we don't need to implement it.
 * @return This function always throws an exception.
 */
Chunk ParquetFlexInputFormat::read()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "read() is not implemented for this input format. Use generate() instead.");
}

// ---- ParquetFlexSchemaReader Implementation ----

/**
 * @brief Constructs a new ParquetFlexSchemaReader
 * 
 * @param buf - Input buffer containing the parquet data
 * @param set - Format settings
 */
ParquetFlexSchemaReader::ParquetFlexSchemaReader(ReadBuffer & buf, const FormatSettings & set)
    : ISchemaReader(buf)
    , settings(set)
{
}

/**
 * @brief Reads and returns the schema of the parquet file
 * 
 * This function constructs the schema by:
 * 1. Adding a timestamp column
 * 2. Adding a JSON-like object column
 * 3. Adding any materialized columns from the settings
 * 
 * @return NamesAndTypesList containing the schema
 */
NamesAndTypesList ParquetFlexSchemaReader::readSchema()
{
    NamesAndTypesList columns;
    columns.emplace_back(TIME_COLUMN_NAME, std::make_shared<DataTypeDateTime64>(6));
    columns.emplace_back(PARSED_COLUMN_NAME, std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));

    for (auto & column : materializationsFromSettings(settings, readParquetHeader(in, settings)))
    {
        columns.emplace_back(column, std::make_shared<DataTypeDynamic>());
    }

    return columns;
}
}

#else

namespace DB
{
void registerInputFormatParquetFlex(FormatFactory & factory)
{
}
}

#endif
