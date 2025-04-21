#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Formats/FormatFactory.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/Object.h>
#include <Poco/NumberParser.h>
#include "Columns/ColumnNullable.h"
#include "Columns/ColumnString.h"
#include "FunctionHelpers.h"

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
/** Converts a CSV string to a JSON string.
  * The function takes two or three arguments:
  * - fieldNames: A comma-separated string of field names (must be a string constant)
  * - csvValue: A comma-separated string of values (nullable)
  * - optional flag to indicate if we want to auto-detect the types of values (default is true)
  *
  * The function returns a JSON string where the field names are mapped to their corresponding values.
  * Unless disabled, the function attempts to detect the types of values and convert them appropriately:
  * - Numbers are converted to JSON numbers
  * - "true" and "false" are converted to JSON booleans
  * - All other values are treated as strings
  * 
  * Example:
  * csvToJSONString('name,age', 'John,42') => '{"name":"John","age":42}'
  * csvToJSONString('name|age', 'John|42') SETTINGS format_csv_delimiter = '|' = '{"name":"John","age":42}'
  */
class FunctionCsvToJsonString final : public IFunction
{
private:
    ContextPtr context;

    /** Splits a CSV string into a vector of strings according to the provided settings.
      * @param s The input CSV string to split
      * @param vectorBuffer A pre-allocated vector to store the split fields
      * @param settings CSV format settings that control parsing behavior (delimiter, quotes, etc)
      */
    static void splitCSV(const StringRef & s, Strings & vectorBuffer, const FormatSettings::CSV & settings)
    {
        ReadBufferFromMemory buf(s.data, s.size);
        vectorBuffer.clear();

        while (!buf.eof())
        {
            String field;
            readCSVString(field, buf, settings);
            vectorBuffer.push_back(field);

            // Skip delimiter if not at end
            if (!buf.eof() && *buf.position() == settings.delimiter)
                ++buf.position();
        }
    }

    /** Merges a vector of field names and values into a JSON String.
      * @param fieldNames A vector of field names
      * @param fieldValues A vector of field values
      * @param settings CSV format settings to get the null representation
      * @param dest A reference to the destination stringstream to store the JSON result
      * @param detectTypes A boolean flag to determine if types should be detected and converted
      */
    static void mergeAsJSON(
        const Strings & fieldNames,
        const Strings & fieldValues,
        const FormatSettings::CSV & settings,
        std::stringstream & dest,
        bool detectTypes)
    {
        const auto n = std::min(fieldNames.size(), fieldValues.size());
        Poco::JSON::Object json;

        for (size_t i = 0; i < n; ++i)
        {
            if (fieldNames.at(i).empty())
            {
                continue;
            }

            auto key = fieldNames.at(i);
            auto value = fieldValues.at(i);

            if (value == settings.null_representation)
            {
                json.set(key, Poco::Dynamic::Var()); // Sets null value
            }
            else
            {
                if (detectTypes)
                {
                    double num;
                    if (Poco::NumberParser::tryParseFloat(value, num))
                    {
                        json.set(key, num);
                    }
                    else if (value == "true")
                    {
                        json.set(key, true);
                    }
                    else if (value == "false")
                    {
                        json.set(key, false);
                    }
                    else
                    {
                        json.set(key, value);
                    }
                }
                else
                {
                    json.set(key, value);
                }
            }
        }

        dest.str(""); // reset std::stringstream
        json.stringify(dest);
    }

public:
    static constexpr auto name = "csvToJSONString";

    explicit FunctionCsvToJsonString(ContextPtr context_)
        : context(std::move(context_))
    {
    }
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionCsvToJsonString>(context); }

    String getName() const override { return name; }

    // this function supports two or three arguments
    size_t getNumberOfArguments() const override { return 0; }
    virtual bool isVariadic() const override { return true; }

    // we do our own null handling for inputs
    virtual bool useDefaultImplementationForNulls() const override { return false; }

    // col:0 is the list of field names, col:2 is a (default true) Boolean to indicate if we want to auto-detect...
    // ...the types of values - both must be constants
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 3)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 2 or 3 arguments, got {}", getName(), arguments.size());
        }

        // First argument must be a non-null string (field names)
        if (!WhichDataType(arguments[0]).isStringOrFixedString())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument (field names) of function {} must be String constant, got {}",
                getName(),
                arguments[0]->getName());
        }

        // Second argument must be (nullable) string (CSV values) or just NULL (nothing)
        if (auto type = WhichDataType(removeNullable(arguments[1])); !(type.isStringOrFixedString() || type.isNothing()))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument (csv values) of function {} must be String or Null, got {}",
                getName(),
                arguments[1]->getName());
        }

        // Third argument (if present) must be a boolean (detectTypes)
        if (arguments.size() == 3 && !WhichDataType(arguments[2]).isUInt8())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Third argument (detectTypes) of function {} must be Boolean, got {}",
                getName(),
                arguments[2]->getName());
        }

        // function either returns nullable string if CSV input is nullable, or a string otherwise
        return WhichDataType(arguments[1]).isNullable() ? makeNullable(std::make_shared<DataTypeString>())
                                                        : std::make_shared<DataTypeString>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!input_rows_count)
        {
            return ColumnString::create(); // fast path for empty input
        }

        FormatSettings::CSV settings = getFormatSettings(context).csv;
        std::stringstream dest;
        bool detectTypes = true;

        // did we receive a non-null option third argument with CSV parsing options?
        if (arguments.size() == 3)
        {
            if (const auto * constOptions = checkAndGetColumnConst<ColumnUInt8>(arguments[2].column.get()); constOptions)
            {
                detectTypes = constOptions->getValue<UInt8>() != 0;
            }
        }

        // we expect the field names to be a string constant
        if (const auto * constFieldNames = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()); constFieldNames)
        {
            const auto fieldNamesStr = constFieldNames->getDataAt(0);
            Strings fieldNames;
            fieldNames.reserve(128);

            // parse out the field names (just once)
            splitCSV(fieldNamesStr, fieldNames, settings);

            // buffer for split CSV values
            Strings vectorBuffer;
            vectorBuffer.reserve(128);

            // csv value also provided as a string constant?
            if (const auto * constCSV = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()); constCSV)
            {
                splitCSV(constCSV->getValue<String>(), vectorBuffer, settings);
                mergeAsJSON(fieldNames, vectorBuffer, settings, dest, detectTypes);
                return DataTypeString().createColumnConst(input_rows_count, dest.str());
            }

            // csv value provided as a string (or nullable string) column
            if (const auto * colCSV = checkAndGetColumn<ColumnString>(removeNullable(arguments[1].column).get()))
            {
                // Check if input is nullable to determine result column type
                const auto * colNullable = typeid_cast<const ColumnNullable *>(arguments[1].column.get());
                const auto * null_map = colNullable ? &colNullable->getNullMapData() : nullptr;

                // Create appropriate string result column type (either nullable or not)
                MutableColumnPtr resultCol;
                if (colNullable)
                {
                    resultCol = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
                }
                else
                {
                    resultCol = ColumnString::create();
                }

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    if (null_map && (*null_map)[i])
                    {
                        resultCol->insertDefault();
                    }
                    else
                    {
                        StringRef input = colCSV->getDataAt(i);
                        splitCSV(input, vectorBuffer, settings);
                        mergeAsJSON(fieldNames, vectorBuffer, settings, dest, detectTypes);
                        resultCol->insertData(dest.str().data(), dest.str().length());
                    }
                }

                return resultCol;
            }

            // col:1 (csv input) is neither String nor ConstString: must be NULL Constant -> return NULL column
            auto null_col = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
            null_col->insertDefault();
            return ColumnConst::create(std::move(null_col), input_rows_count);
        }

        // we should never get here as the framework ensures that col:0 and col:2 are constant
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Function {} failed to validate arguments", getName());
    }
};

}

REGISTER_FUNCTION(csvToJSONString)
{
    factory.registerFunction<FunctionCsvToJsonString>();
}
}
