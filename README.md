# ByteForge- Building Database Engine

This is my learning toy project for understanding how databases work, written in Go. It's basically a playground for trying out table creation, CRUD, binary file storage, and some indexing. I tried to stick to Go's standard library style, but it's mostly for fun and learning.

## Roadmap (aka What I Plan To Try Next)
- Try around with data structures and indexing (Arrays, Linked Lists, Trees, B/B+ Trees)
- Use TLV encoding and a file/folder structure kind of like PostgreSQL
- Get basic CRUD working with simple storage and retrieval
- Add Write-Ahead Logging (WAL) so I don't lose data if it crashes
- Try out data paging, B-Tree indexing, and maybe an LRU buffer pool
- Add some kind of full-text search for string columns

This project is just me figuring out how real databases do their thing, step by step. If it breaks, that's part of the fun!

## Features

- Create and manage a simple database (`my_db`)
- Table creation with customizable columns and types
- Insert, update, delete, and query records
- Data persistence using binary files
- Simple command-line interface for testing

## Project Structure

```
.
├── cmd/         # Main entry point (main.go)
├── internal/
│   ├── db.go    # Core database logic
│   ├── errors.go
│   ├── platform/
│   └── table/
└── data/        # Data files are created here at runtime
```

## Getting Started

### Prerequisites

- Go 1.23 or newer

### Running the Project

1. **Clone the repository and navigate to the project directory.**
2. **Run the main program:**
   ```bash
   go run ./cmd/main.go
   ```
   This will create a database, a table, insert some users, and print them.

3. **Test reading and inserting more:**
   ```bash
   go run ./cmd/main.go r
   ```

## How It Works

- The program creates a database and a `users` table on first run.
- It inserts sample user records and queries them.
- Data is stored in the `./data/my_db/` directory as binary files.

## Customization

You can modify `cmd/main.go` to test other operations like update, delete, or bulk insert.

## License

This project is for learning purposes. 