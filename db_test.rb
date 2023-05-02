describe "database" do

    before do
        `rm -rf test.db`
    end

    def run_script(commands)
        raw_output = nil
        IO.popen("./db test.db", "r+") do |pipe|
            commands.each do |command|
                pipe.puts command
            end

            pipe.close_write

            # read entire output
            raw_output = pipe.gets(nil)
        end
        raw_output.split("\n")
    end

    # test case for unrecognized command
    it 'test exit and unrecognized command and sql sentence' do
        result = run_script([
            "hello world",
            ".HELLO WORLD",
            ".exit",
        ])
        expect(result).to match_array([
            "db > Unrecognized keyword at start of 'hello world'.",
            "db > Unrecognized command: .HELLO WORLD",
            "db > Bye!",
        ])
    end

    # test case for insert and select operation
    it 'inserts and retrieves a row' do
        result = run_script([
            "insert 1 user1 person1@example.com",
            "insert 2 user2",
            "select",
            ".exit",
        ])
        expect(result).to match_array([
            "db > Executed.",
            "db > Syntax error. Could not parse statement.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db > Bye!",
        ])
    end

    # test case for corner cases / when table is full
    it 'prints error message when table is full' do
        script = (1..900).map do |i|
            "insert #{i} user#{i} person#{i}@example.com"
        end
        script << ".exit"
        result = run_script(script)
        expect(result.last(2)).to match_array([
            "db > Executed.",
            "db > Need to implement searching an internal node.",
        ])
    end

    # test case for corner cases / when entered string is at maximum
    it 'allows inserting strings that are the maximum length' do
        long_username = "a"*32
        long_email = "a"*255
        script = [
            "insert 1 #{long_username} #{long_email}",
            "select",
            ".exit",
        ]
        result = run_script(script)
        expect(result).to match_array([
            "db > Executed.",
            "db > (1, #{long_username}, #{long_email})",
            "Executed.",
            "db > Bye!",
        ])
    end

    # test case for corner cases / when entered string is more than maximum
    it 'prints error message if strings are too long' do
        long_username = "a"*33
        long_email = "a"*256
        script = [
            "insert 1 #{long_username} #{long_email}",
            "select",
            ".exit",
        ]
        result = run_script(script)
        expect(result).to match_array([
            "db > String is too long.",
            "db > Executed.",
            "db > Bye!",
        ])
    end

    # test case for corner cases / when entered string is negative value
    it 'prints an error message if id is negative' do
        script = [
            "insert -1 cstack foo@bar.com",
            "select",
            ".exit",
        ]
        result = run_script(script)
        expect(result).to match_array([
            "db > ID must be positive.",
            "db > Executed.",
            "db > Bye!",
        ])
    end

    # test case for store data upon exiting program
    it "keeps data after closing connection" do
        result1 = run_script([
            "insert 1 user1 person1@example.com",
            ".exit",
        ])
        expect(result1).to match_array([
            "db > Executed.",
            "db > Bye!",
        ])
        result2 = run_script([
            "select",
            ".exit",
        ])
        expect(result2).to match_array([
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db > Bye!",
        ])
    end

    # test case for b tree implementation
    it "allows printing out the structure of a one-node btree" do
        script = [3, 1, 2].map do |i|
            "insert #{i} user#{i} person#{i}@example.com"
        end
        script << ".btree"
        script << ".exit"
        result = run_script(script)

        expect(result).to match_array([
                            "db > Executed.",
                            "db > Executed.",
                            "db > Executed.",
                            "db > Tree:",
                            "- leaf (size 3)",
                            "  - 1",
                            "  - 2",
                            "  - 3",
                            "db > Bye!",
        ])
    end

    it "prints constants" do
        script = [
            ".constants",
            ".exit",
        ]
        result = run_script(script)

        expect(result).to match_array([
                            "db > Constants:",
                            "ROW_SIZE: 293",
                            "COMMON_NODE_HEADER_SIZE: \u0006",
                            "LEAF_NODE_HEADER_SIZE: 10",
                            "LEAF_NODE_CELL_SIZE: 297",
                            "LEAF_NODE_SPACE_FOR_CELLS: 4086",
                            "LEAF_NODE_MAX_CELLS: 13",
                            "db > Bye!",
        ])
    end

    # test case for duplicated id in database
    it "prints an error message if there is a duplicate id" do
        script = [
            "insert 1 user1 person1@example.com",
            "insert 1 user1 person1@example.com",
            "select",
            ".exit",
        ]
        result = run_script(script)
        expect(result).to match_array([
                            "db > Executed.",
                            "db > Error: Duplicate key.",
                            "db > (1, user1, person1@example.com)",
                            "Executed.",
                            "db > Bye!",
        ])
    end

    it "allows printing out the structure of a 3-leaf-node btree" do
        script = (1..14).map do |i|
            "insert #{i} user#{i} person#{i}@example.com"
        end
        script << ".btree"
        script << "insert 15 user15 person15@example.com"
        script << ".exit"
        result = run_script(script)

        expect(result[14...(result.length)]).to match_array([
            "db > Tree:",
            "- internal (size 1)",
            "  - leaf (size 7)",
            "    - 1",
            "    - 2",
            "    - 3",
            "    - 4",
            "    - 5",
            "    - 6",
            "    - 7",
            "  - key 7",
            "  - leaf (size 7)",
            "    - 8",
            "    - 9",
            "    - 10",
            "    - 11",
            "    - 12",
            "    - 13",
            "    - 14",
            "db > Need to implement searching an internal node.",
        ])
    end

end