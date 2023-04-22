#include<iostream>
#include<string>

using namespace std;

class DB
{
public:
    void start();
    void print_prompt();

    bool parse_meta_command(string command);
};

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
    }
    
}

void DB::print_prompt()
{
    cout << "db > ";
}

bool DB::parse_meta_command(string command)
{
    if(command == ".exit")
    {
        cout << "Bye!" << endl;
        exit(EXIT_SUCCESS);
    }
    else
    {
        cout << "Unrecognized command: " << command << endl;
        return true;
    }
    
    return false;
}

int main()
{
    DB db;
    db.start();
}