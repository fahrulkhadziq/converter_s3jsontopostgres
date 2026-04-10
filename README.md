- clone https://github.com/fahrulkhadziq/converter_s3jsontopostgres.git
- go mod tidy
- create .env
- fill the .env file with :
  DB_USER=
  DB_PASSWORD=
  DB_NAME=
  DB_HOST=
  DB_PORT=
  DB_SSLMODE=
- If you want to use it in main.go then edit the table name and structure, just adjust it to your own.
- If it is correct, just run go run main.go
