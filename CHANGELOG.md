# Changelog

## `b41c431` Code improvements

1. Better naming for struct fields, structs, funcs, ...
2. Split all the processing logic from the `main()` function into separate specialized structs:
    - `TaskProducer`: generates tasks (some of which may eventually fail)
    - `TaskProcessor`: processes tasks (simulates heavy work with a sleep)
    - `TaskFilter`: separates successful and failed tasks
    - `TaskStorage`: accumulates successful tasks and errors, for later output
3. Added logging via `github.com/sirupsen/logrus` for debugging purposes.
4. Task status differentiation (instead of a string-based approach)
5. Goroutines are synchronized via `sync.WaitGroup`, i.e. the `main()` function waits for all spawned routines to finish.
6. Graceful shutdown of all goroutines after a 3-second period (via `context.Context`)
