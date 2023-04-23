#include<iostream>
#include<string>

using namespace std;

enum MetaCommandResult
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};

enum PrepareResult
{
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
};

enum StatementType
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
};

class Statement
{
public:
    StatementType type;
};

class DB
{
public:
    void start();
    void print_prompt();

    bool parse_meta_command(string &command);
    MetaCommandResult do_meta_command(string &command);

    PrepareResult prepare_statement(string &inputLine, Statement &statement);
    bool parse_statement(string &inputLine, Statement &statement);
    void execute_statement(Statement &statement);

};

void DB::print_prompt()
{
    cout << "db > ";
}

bool DB::parse_meta_command(string &command)
{
    if(command[0] == '.')
    {
        switch(do_meta_command(command))
        {
        case META_COMMAND_SUCCESS:
            return true;
        case META_COMMAND_UNRECOGNIZED_COMMAND:
            cout << "Unrecognized command: " << command << endl;
            return true;
        }
    }
    
    return false;
}


MetaCommandResult DB::do_meta_command(string &command)
{
    if (command == ".exit")
    {
        cout << "Bye!" << endl;
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult DB::prepare_statement(string &inputLine, Statement &statement)
{
    if(!inputLine.compare(0, 6, "insert"))
    {
        statement.type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    else if(!inputLine.compare(0, 6, "select"))
    {
        statement.type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    else
    {
        return PREPARE_UNRECOGNIZED_STATEMENT;
    }
}

bool DB::parse_statement(string &inputLine, Statement &statement)
{
    switch (prepare_statement(inputLine, statement))
    {
        case PREPARE_SUCCESS:
            return false;
        case PREPARE_UNRECOGNIZED_STATEMENT:
            cout << "Unrecognized keyword at start of '" << inputLine << "'." << endl;
            return true;
    }
    return false;
}

void DB::execute_statement(Statement &statement)
{
    switch (statement.type)
    {
        case STATEMENT_INSERT:
            cout << "Executing insert statement" << endl;
            break;
        case STATEMENT_SELECT:
            cout << "Executing select statement" << endl;
            break;
    }
}

void DB::start()
{
    while (true)
    {
        print_prompt();

        string inputLine;
        getline(cin, inputLine);

        if(parse_meta_command(inputLine))
        {
            continue;
        }

        Statement statement;

        if(parse_statement(inputLine, statement))
        {
            continue;
        }

        execute_statement(statement);
    }
}

int main()
{
    DB db;
    db.start();
}