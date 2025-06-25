# ByteForge – A Simple Database Engine for Learning

ByteForge is a simple database project written in Go. It lets you create tables, add and search users, and see how databases work under the hood. This is a learning project, so it's made to be easy to read and try out.

## Features
- Create a database and tables
- Insert, update, delete, and search records
- Data is saved in files so it isn't lost
- Full-text search index for string columns

This project is just me figuring out how real databases do their thing, step by step. If it breaks, that's part of the fun!

## How to Run

1. **Open a terminal and go to the project folder.**
2. **Run the main program:**
   ```bash
   go run ./cmd/main.go
   ```
   This will create a database, a table, add some users, and print them out. It will also show the full-text search index.
3. **Other commands:**
   - `go run ./cmd/main.go r` — Read and print all users
   - `go run ./cmd/main.go t` — Test full-text index marshaling

---

**Summary Table:**

| Command                        | What it does                                 |
|--------------------------------|----------------------------------------------|
| go run ./cmd/main.go           | Create table, insert, print fulltext index   |
| go run ./cmd/main.go c         | Same as above                                |
| go run ./cmd/main.go r         | Read and print all users                     |
| go run ./cmd/main.go t         | Test fulltext index marshaling               |

## Want to Play More?

## How It Works (Quickly)
- The program makes a database and a `users` table.
- It adds some sample users and shows them to you.
- All data is saved in the `./data/my_db/` folder as files.

## License

This project is for learning purposes. 