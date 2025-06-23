# ByteForge – A Simple Database Engine for Learning

This is my learning toy project for understanding how databases work, written in Go. It's basically a playground for trying out table creation, CRUD, binary file storage, and some indexing (including B-tree indexes). I tried to stick to Go's standard library style, but it's mostly for fun and learning.

## What I'm Trying to Build (Roadmap)
- Play with different data structures (Arrays, Linked Lists, Trees, B/B+ Trees)
- Use TLV encoding and a file/folder setup inspired by PostgreSQL
- Get basic CRUD (Create, Read, Update, Delete) working
- Add Write-Ahead Logging (WAL) so data isn't lost if the program crashes
- Try paging, B-Tree indexing, and maybe an LRU buffer pool
- Add simple full-text search for string columns

This project is just me figuring out how real databases do their thing, step by step. If it breaks, that's part of the fun!

## What Can It Do?

- Make and manage a simple database (called `my_db`)
- Create tables with columns and types you choose
- Insert, update, delete, and search for records
- Save data in binary files so it's not lost when you quit
- Test everything with a simple command-line interface

## How Indexing Works
- Uses B-tree indexes (with Google's btree package) for fast primary key lookups
- Each table has its own index, rebuilt in memory when you start the program
- Index files use TLV encoding
- Only primary key (id) searches are fast; range searches aren't optimized
- The whole index must fit in memory

## What You Need
- Go 1.23 or newer

## How to Run It

1. **Clone this repo and go to the project folder.**
2. **Run the main program:**
   ```bash
   go run ./cmd/main.go
   ```
   This will create a database, a table, add some users, and print them out.

3. **Try reading and adding more users:**
   ```bash
   go run ./cmd/main.go r
   ```

## How It Works (In Simple Terms)

- On the first run, the program makes a database and a `users` table.
- It adds some sample users and shows them to you.
- All data is saved in the `./data/my_db/` folder as binary files.

## Want to Play More?

You can change `cmd/main.go` to try updates, deletes, or bulk inserts. Tweak and experiment as much as you want!

## License

This project is for learning purposes. 