#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/StorageValues.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include "registerTableFunctions.h"

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class TableFunctionCriblFlex : public ITableFunction
{
private:
    constexpr static auto time_column_name = "time";
    constexpr static auto data_type_column_name = "dataType";
    constexpr static auto raw_column_name = "raw";
    constexpr static auto parsed_data_column_name = "parsedData";

public:
    constexpr static auto name = "criblFlex";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
    const char * getStorageTypeName() const override { return "CriblFlex"; }

    ColumnsDescription getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const override
    {
        NamesAndTypesList columns;
        columns.emplace_back(time_column_name, std::make_shared<DataTypeDateTime64>(6));
        columns.emplace_back(data_type_column_name, std::make_shared<DataTypeString>());
        columns.emplace_back(raw_column_name, std::make_shared<DataTypeString>());
        columns.emplace_back(parsed_data_column_name, std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));
        return ColumnsDescription(columns);
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr /*context*/) override
    {
        // No arguments needed for this table function
        if (ast_function->children.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires no arguments", getName());
    }

    StoragePtr executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription /* cached_columns */,
        bool /* is_insert_query */) const override
    {
        auto columns = getActualTableStructure(context, false);

        // Create a block with our sample data
        Block block;

        // Create columns
        auto time_column = DataTypeDateTime64(6).createColumn();
        auto data_type_column = DataTypeString().createColumn();
        auto raw_column = DataTypeString().createColumn();
        auto parsed_data_column = DataTypeObject(DataTypeObject::SchemaFormat::JSON).createColumn();

        // Add sample data
        assert_cast<DataTypeDateTime64::ColumnType &>(*time_column).insertValue(1746259200000000); // 2025-05-02 00:00:00.000000
        data_type_column->insert("aws_vpcflow");
        raw_column->insert("2 602320997947 eni-0b2fc5457066bc156 159.203.36.225 10.0.0.236 6500 4200 6 22 1056 1746208185 1746208190 REJECT OK");
        {
            Object obj;
            obj["account_id"] = 602320997947;
            obj["action"] = "REJECT";
            obj["bytes"] = 1056;
            obj["dstaddr"] = "10.0.0.236";
            obj["dstport"] = 4200;
            obj["end"] = 1746208190;
            obj["interface_id"] = "eni-0b2fc5457066bc156";
            parsed_data_column->insert(obj);
        }

        assert_cast<DataTypeDateTime64::ColumnType &>(*time_column).insertValue(1746262800000000); // 2025-05-02 01:00:00.000000
        data_type_column->insert("syslog");
        raw_column->insert("<4>May 02 17:39:54 rowe5387 consequuntur[6348]: Try to copy the AI card, maybe it will bypass the virtual bus!");
        {
            Object obj;
            obj["appname"] = "consequuntur";
            obj["hostname"] = "hostname";
            obj["message"] = "Try to copy the AI card, maybe it will bypass the virtual bus!";
            Object process;
            process["pid"] = 6348;
            process["priority"] = 4;
            obj["process"] = process;
            parsed_data_column->insert(obj);
        }

        assert_cast<DataTypeDateTime64::ColumnType &>(*time_column).insertValue(1746266400000000); // 2025-05-02 02:00:00.000000
        data_type_column->insert("apache_access");
        raw_column->insert("70.254.161.129 - jones5838 [02/May/2025:17:30:00 +0000] \"POST /action-items/monetize/frictionless/orchestrate\" 504 25681");
        {
            Object obj;
            obj["bytes"] = 25681;
            obj["clientip"] = "70.254.161.129";
            obj["request"] = "POST /action-items/monetize/frictionless/orchestrate";
            obj["status"] = 504;
            obj["username"] = "jones5838";
            parsed_data_column->insert(obj);
        }

        // Add columns to block
        block.insert(ColumnWithTypeAndName(std::move(time_column), std::make_shared<DataTypeDateTime64>(6), time_column_name));
        block.insert(ColumnWithTypeAndName(std::move(data_type_column), std::make_shared<DataTypeString>(), data_type_column_name));
        block.insert(ColumnWithTypeAndName(std::move(raw_column), std::make_shared<DataTypeString>(), raw_column_name));
        block.insert(ColumnWithTypeAndName(
            std::move(parsed_data_column), std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON), parsed_data_column_name));

        auto res = std::make_shared<StorageValues>(StorageID(getDatabaseName(), table_name), columns, block);
        res->startup();
        return res;
    }
};


void registerTableFunctionCriblFlex(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCriblFlex>();
}

}
