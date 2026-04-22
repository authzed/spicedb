# Test Runner Detection and Commands

## Command templates

### go test (Go)

```bash
# Run single test 
 go test -run "TEST_NAME" -v ./PACKAGE/ 2>&1 
  
 # Run N times 
 go test -run "TEST_NAME" -v -count=N ./PACKAGE/ 2>&1 
  
 # Run with race detector 
 go test -run "TEST_NAME" -v -race ./PACKAGE/ 2>&1 
  
 # Run with timeout 
 go test -run "TEST_NAME" -v -timeout 30s ./PACKAGE/ 2>&1 
  
 # Shuffle order 
 go test -v -shuffle=on ./PACKAGE/ 2>&1 
 
# run until failure is seen or until 4 hours have elapsed, whichever comes first.
go test ./PACKAGE/ -race -c && stress ./PACKAGE.test -test.run TEST_NAME
```

## Parsing exit codes

All runners use exit code conventions:
- `0` = all tests passed
- `1` or non-zero = at least one test failed

When parsing multi-run output, count exits by grepping for `EXIT: 0` (pass) vs `EXIT: [^0]` (fail).
