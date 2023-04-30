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
    EXECUTE_TABLE_FULL,
    EXECUTE_DUPLICATE_KEY
};

enum NodeType
{
    NODE_INTERNAL,
    NODE_LEAF
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
private:
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
    uint32_t num_pages;

public:
    Pager(const char *filename);

    void *get_page(uint32_t page_num);
    void pager_flush(uint32_t page_num);

    friend class Table;
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
    num_pages = file_length / PAGE_SIZE;

    if(file_length % PAGE_SIZE != 0)
    {
        cerr << "Db file is not a whole number of pages. Corrupt file." << endl;
        exit(EXIT_FAILURE);
    }

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
             << TABLE_MAX_PAGES << endl;
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

        if(page_num >= num_pages)
        {
            this->num_pages = page_num + 1;
        }
    }

    return pages[page_num];

}

void Pager::pager_flush(uint32_t page_num)
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

    ssize_t bytes_written = write(file_descriptor, pages[page_num], PAGE_SIZE);

    if(bytes_written == -1)
    {
        cout << "Error writing: " << errno << endl;
        exit(EXIT_FAILURE);
    }
}

// Common Node Header Layout
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf Node Header Layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf Node Body Layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

class LeafNode
{
private:
    void *node;
public:
    LeafNode(void *node) : node(node){}

    void initialize_leaf_node()
    {
        set_node_type(NODE_LEAF);
        *leaf_node_num_cells() = 0;
    }

    uint32_t *leaf_node_num_cells()
    {
        return (uint32_t *)((char *)node + LEAF_NODE_NUM_CELLS_OFFSET);
    }

    void *leaf_node_cell(uint32_t cell_num)
    {
        return (char *)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
    }

    uint32_t *leaf_node_key(uint32_t cell_num)
    {
        return (uint32_t *)leaf_node_cell(cell_num);
    }

    void *leaf_node_value(uint32_t cell_num)
    {
        return (char *)leaf_node_cell(cell_num) + LEAF_NODE_KEY_SIZE;
    }

    void print_leaf_node()
    {
        uint32_t num_cells = *leaf_node_num_cells();
        cout << "leaf (size " << num_cells << ")" << endl;
        for(uint32_t i = 0; i < num_cells; i++)
        {
            uint32_t key = *leaf_node_key(i);
            cout << "  - " << i << " : " << key << endl;
        }
    }

    NodeType get_node_type()
    {
        uint8_t value = *((uint8_t *)((char *)node + NODE_TYPE_OFFSET));
        return (NodeType)value;
    }

    void set_node_type(NodeType type)
    {
        *((uint8_t *)((char *)node + NODE_TYPE_OFFSET)) = (uint8_t)type;
    }
};

class Table;
class Cursor
{
private:
    Table *table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;

public:
    Cursor(Table *table);
    Cursor(Table *table, uint32_t page_num, uint32_t key);
    void *cursor_value();
    void cursor_advance();
    void leaf_node_insert(uint32_t key, Row &value);

    friend class DB;
};

class Table
{
private:
    uint32_t root_page_num;
    Pager pager;
public:
    Table(const char *filename) : pager(filename)
    {
        root_page_num = 0;
        if(pager.num_pages == 0)
        {
            // New file. Initialize page 0 as leaf node.
            LeafNode root_node = pager.get_page(0);
            root_node.initialize_leaf_node();
        }
    }
    Cursor *table_find(uint32_t key);
    ~Table();

    friend class Cursor;
    friend class DB;
};

Cursor::Cursor(Table *table)
{
    this->table = table;
    page_num = table->root_page_num;
    LeafNode root_node = table->pager.get_page(page_num);
    uint32_t num_cells = *root_node.leaf_node_num_cells();

    // start at the beginning of the table
    cell_num = 0;
    end_of_table = (num_cells == 0);
}

Cursor::Cursor(Table *table, uint32_t page_num, uint32_t key)
{
    this->table = table;
    this->page_num = page_num;
    this->end_of_table = false;

    LeafNode root_node = table->pager.get_page(page_num);
    uint32_t num_cells = *root_node.leaf_node_num_cells();
    
    // Binary Search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while(one_past_max_index != min_index)
    {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *root_node.leaf_node_key(index);
        if(key == key_at_index)
        {
            this->cell_num = index;
            return;
        }
        if(key < key_at_index)
        {
            one_past_max_index = index;
        }
        else
        {
            min_index = index + 1;
        }
    }

    this->cell_num = min_index;
}

void *Cursor::cursor_value()
{
    void *page = table->pager.get_page(page_num);

    return LeafNode(page).leaf_node_value(cell_num);
}

void Cursor::cursor_advance()
{
    LeafNode leaf_node = table->pager.get_page(page_num);
    cell_num += 1;
    if(cell_num >= *leaf_node.leaf_node_num_cells())
    {
        end_of_table = true;
    }
}

void Cursor::leaf_node_insert(uint32_t key, Row &value)
{
    LeafNode leaf_node = table->pager.get_page(page_num);
    uint32_t num_cells = *leaf_node.leaf_node_num_cells();

    if(num_cells >= LEAF_NODE_MAX_CELLS)
    {
        // Node full
        cout << "Need to implement splitting a leaf node." << endl;
        exit(EXIT_FAILURE);
    }

    if(cell_num < num_cells)
    {
        //make room for new cell
        for(uint32_t i = num_cells; i > cell_num; i--)
        {
            memcpy(leaf_node.leaf_node_cell(i), leaf_node.leaf_node_cell(i - 1),
                    LEAF_NODE_CELL_SIZE);
        }
    }

    // insert new cell
    *(leaf_node.leaf_node_num_cells()) += 1;
    *(leaf_node.leaf_node_key(cell_num)) = key;
    serialize_row(value, leaf_node.leaf_node_value(cell_num));
}

Cursor *Table::table_find(uint32_t key)
{
    LeafNode root_node = pager.get_page(root_page_num);

    if(root_node.get_node_type() == NODE_LEAF)
    {
        return new Cursor(this, root_page_num, key);
    }
    else
    {
        cout << "Need to implement searching internal nodes." << endl;
        exit(EXIT_FAILURE);
    }
}

Table::~Table()
{
    for(uint32_t i = 0; i < pager.num_pages; i++)
    {
        if(pager.pages[i] == nullptr)
        {
            continue;
        }
        pager.pager_flush(i);
        free(pager.pages[i]);
        pager.pages[i] = nullptr;
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
    else if(command == ".btree")
    {
        cout << "Tree:" << endl;
        LeafNode root_node = table->pager.get_page(table->root_page_num);
        root_node.print_leaf_node();
        return META_COMMAND_SUCCESS;
    }
    else if(command == ".constants")
    {
        cout << "Constants:" << endl;
        cout << "ROW_SIZE: " << ROW_SIZE << endl;
        cout << "COMMON_NODE_HEADER_SIZE: " << COMMON_NODE_HEADER_SIZE << endl;
        cout << "LEAF_NODE_HEADER_SIZE: " << LEAF_NODE_HEADER_SIZE << endl;
        cout << "LEAF_NODE_CELL_SIZE: " << LEAF_NODE_CELL_SIZE << endl;
        cout << "LEAF_NODE_SPACE_FOR_CELLS: " << LEAF_NODE_SPACE_FOR_CELLS << endl;
        cout << "LEAF_NODE_MAX_CELLS: " << LEAF_NODE_MAX_CELLS << endl;
        return META_COMMAND_SUCCESS;
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
    LeafNode leaf_node = table->pager.get_page(table->root_page_num);
    uint32_t num_cells = *leaf_node.leaf_node_num_cells();
    if(num_cells >= LEAF_NODE_MAX_CELLS)
    {
        cout << "Leaf node full." << endl;
        return EXECUTE_TABLE_FULL;
    }

    Cursor *cursor = table->table_find(statement.row_to_insert.id);

    if(cursor->cell_num < num_cells)
    {
        uint32_t key_at_index = *leaf_node.leaf_node_key(cursor->cell_num);
        if(key_at_index == statement.row_to_insert.id)
        {
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    cursor->leaf_node_insert(statement.row_to_insert.id, statement.row_to_insert);

    delete cursor;

    return EXECUTE_SUCCESS;
}

ExecuteResult DB::execute_select(Statement &statement)
{
    // start of the table
    Cursor *cursor = new Cursor(table);

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
        case (EXECUTE_DUPLICATE_KEY):
            cout << "Error: Duplicate key." << endl;
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