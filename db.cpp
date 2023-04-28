#include <iostream>
#include <cstring>
#include <string>

#include<fcntl.h>
#include<unistd.h>

using namespace std;

enum MetaCommandResult
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};

enum PrepareResult
{
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
};

enum StatementType
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
};

enum ExecuteResult
{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
};

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

class Row
{
public:

    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];

    Row()
    {
        id = 0;
        username[0] = '\0';
        email[0] = '\0';
    }

    Row(uint32_t id, const char *username, const char *email)
    {
        this->id = id;
        strncpy(this->username, username, COLUMN_USERNAME_SIZE + 1);
        strncpy(this->email, email, COLUMN_EMAIL_SIZE + 1);
    }
};

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void serialize_row(Row &source, void *destination)
{
    memcpy((char *)destination + ID_OFFSET, &(source.id), ID_SIZE);
    memcpy((char *)destination + USERNAME_OFFSET, &(source.username), USERNAME_SIZE);
    memcpy((char *)destination + EMAIL_OFFSET, &(source.email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row &destination)
{
    memcpy(&(destination.id), (char *)source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination.username), (char *)source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination.email), (char *)source + EMAIL_OFFSET, EMAIL_SIZE);
}

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

class Pager
{
public:
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];

    Pager(const char *filename);
    void *get_page(uint32_t page_num);
    void pager_flush(uint32_t page_num, uint32_t size);
};

Pager::Pager(const char *filename)
{
    file_descriptor = open(filename,
                           O_RDWR |       // Read/Write mode
                               O_CREAT,   // Create file if it does not exist
                           S_IWUSR |      // User write permission
                               S_IRUSR    // User read permission
    );
    if(file_descriptor < 0)
    {
        cerr << "Error: cannot open file " << filename << endl;
        exit(EXIT_FAILURE);
    }
    file_length = lseek(file_descriptor, 0, SEEK_END);

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        pages[i] = nullptr;
    }
}

void *Pager::get_page(uint32_t page_num)
{
    if(page_num > TABLE_MAX_PAGES)
    {
        cout << "Tried to fetch page numbe out of bounds. " << page_num << " > "
             << TABLE_MAX_ROWS << endl;
        exit(EXIT_FAILURE);
    }

    if(pages[page_num] == nullptr)
    {
        // Cache miss. Allocate memory and load from file
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = file_length / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if(file_length % PAGE_SIZE)
        {
            num_pages += 1;
        }

        if(page_num <= num_pages)
        {
            lseek(file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(file_descriptor, page, PAGE_SIZE);
            if(bytes_read == -1)
            {
                cout << "Error reading file: " << errno << endl;
                exit(EXIT_FAILURE);
            }
        }

        pages[page_num] = page;
    }

    return pages[page_num];

}

void Pager::pager_flush(uint32_t page_num, uint32_t size)
{
    if(pages[page_num] == nullptr)
    {
        cout << "Tried to flush null page" << endl;
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if(offset == -1)
    {
        cout << "Error seeking: " << errno << endl;
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(file_descriptor, pages[page_num], size);

    if(bytes_written == -1)
    {
        cout << "Error writing: " << errno << endl;
        exit(EXIT_FAILURE);
    }
}

class Table
{
public:
    uint32_t num_rows;
    Pager pager;
    Table(const char *filename) : pager(filename)
    {
        num_rows = pager.file_length / ROW_SIZE;
    }
    ~Table();

};

Table::~Table()
{
    uint32_t num_full_pages = num_rows / ROWS_PER_PAGE;

    for(uint32_t i = 0; i < num_full_pages; i++)
    {
        if(pager.pages[i] == nullptr)
        {
            continue;
        }
        pager.pager_flush(i, PAGE_SIZE);
        free(pager.pages[i]);
        pager.pages[i] = nullptr;
    }

    // There may be a partial page to write to the end of the file
    // This should not be needed after we switch to a B-tree
    uint32_t num_additional_rows = num_rows % ROWS_PER_PAGE;
    if(num_additional_rows > 0)
    {
        uint32_t page_num = num_full_pages;
        if(pager.pages[page_num] != nullptr)
        {
            pager.pager_flush(page_num, num_additional_rows * ROW_SIZE);
            free(pager.pages[page_num]);
            pager.pages[page_num] = nullptr;
        }
    }

    int result = close(pager.file_descriptor);
    if(result == -1)
    {
        cout << "Error closing db file." << endl;
        exit(EXIT_FAILURE);
    }
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        void *page = pager.pages[i];
        if(page)
        {
            free(page);
            pager.pages[i] = nullptr;
        }
    }
}

class Cursor
{
public:
    Table *table;
    uint32_t row_num;
    bool end_of_table;

    Cursor(Table *&table, bool option);
    void *cursor_value();
    void cursor_advance();
};

Cursor::Cursor(Table *&table, bool option)
{
    this->table = table;
    if(option)
    {
        // start at the beginning of the table
        row_num = 0;
        end_of_table = (table->num_rows == 0);
    }
    else
    {
        // end of the table
        row_num = table->num_rows;
        end_of_table = true;
    }
}

void *Cursor::cursor_value()
{
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pager.get_page(page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return (char *)page + byte_offset;
}

void Cursor::cursor_advance()
{
    row_num += 1;
    if(row_num >= table->num_rows)
    {
        end_of_table = true;
    }
}

class Statement
{
public:
    StatementType type;
    Row row_to_insert;
};

class DB
{
private:
    Table *table;

public:
    DB(const char *filename)
    {
        table = new Table(filename);
    }
    void start();
    void print_prompt();

    bool parse_meta_command(string &command);
    MetaCommandResult do_meta_command(string &command);

    PrepareResult prepare_insert(string &inputLine, Statement &statement);
    PrepareResult prepare_statement(string &inputLine, Statement &statement);
    bool parse_statement(string &inputLine, Statement &statement);
    void execute_statement(Statement &statement);
    ExecuteResult execute_insert(Statement &statement);
    ExecuteResult execute_select(Statement &statement);

    ~DB()
    {
        delete table;
    }
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
        delete(table);
        cout << "Bye!" << endl;
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult DB::prepare_insert(string &inputLine, Statement &statement)
{
    statement.type = STATEMENT_INSERT;

    char *insert_line = (char *) inputLine.c_str();
    char *keyword = strtok(insert_line, " ");
    char *id_string = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if(id_string == NULL || username == NULL || email == NULL)
    {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = atoi(id_string);
    if(id < 0)
    {
        return PREPARE_NEGATIVE_ID;
    }
    if(strlen(username) > COLUMN_USERNAME_SIZE)
    {
        return PREPARE_STRING_TOO_LONG;
    }
    if(strlen(email) > COLUMN_EMAIL_SIZE)
    {
        return PREPARE_STRING_TOO_LONG;
    }
    statement.row_to_insert = Row(id, username, email);

    return PREPARE_SUCCESS;

}

PrepareResult DB::prepare_statement(string &inputLine, Statement &statement)
{
    if(!inputLine.compare(0, 6, "insert"))
    {
        return prepare_insert(inputLine, statement);
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
    switch(prepare_statement(inputLine, statement))
    {
        case PREPARE_SUCCESS:
            return false;
        case PREPARE_NEGATIVE_ID:
            cout << "ID must be positive." << endl;
            return true;
        case PREPARE_STRING_TOO_LONG:
            cout << "String is too long." << endl;
            return true;
        case PREPARE_SYNTAX_ERROR:
            cout << "Syntax error. Could not parse statement." << endl;
            return true;
        case PREPARE_UNRECOGNIZED_STATEMENT:
            cout << "Unrecognized keyword at start of '" << inputLine << "'." << endl;
            return true;
    }
    return false;
}

ExecuteResult DB::execute_insert(Statement &statement)
{
    if(table->num_rows >= TABLE_MAX_ROWS)
    {
        cout << "Error: Table full." << endl;
        return EXECUTE_TABLE_FULL;
    }

    //end of the table
    Cursor *cursor = new Cursor(table, false);

    serialize_row(statement.row_to_insert, cursor->cursor_value());
    table->num_rows++;

    delete cursor;

    return EXECUTE_SUCCESS;
}

ExecuteResult DB::execute_select(Statement &statement)
{
    // start of the table
    Cursor *cursor = new Cursor(table, true);

    Row row;
    while(!cursor->end_of_table)
    {
        deserialize_row(cursor->cursor_value(), row);
        cout << "(" << row.id << ", " << row.username << ", " << row.email << ")" << endl;
        cursor->cursor_advance();
    }

    delete cursor;

    return EXECUTE_SUCCESS;
}

void DB::execute_statement(Statement &statement)
{
    ExecuteResult result;
    switch (statement.type)
    {
        case STATEMENT_INSERT:
            result = execute_insert(statement);
            break;
        case STATEMENT_SELECT:
            result = execute_select(statement);
            break;
    }

    switch (result)
    {
        case EXECUTE_SUCCESS:
            cout << "Executed." << endl;
            break;
        case EXECUTE_TABLE_FULL:
            cout << "Error: Table full." << endl;
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

int main(int argc, char const *argv[])
{
    if(argc < 2)
    {
        cout << "Must supply a database filename." << endl;
        exit(EXIT_FAILURE);
    }

    DB db(argv[1]);
    db.start();
}