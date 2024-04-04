# Changelog

## `3ef7cf3` Commented code

Added detailed comments and documentation throughout the code...

## `14ffe2c` Fixed deadlock

Fixed a deadlock in the `TaskProducer`.

This deadlock occurred when all remaining goroutines received a `cancel` event from the `context`, but the `TaskProducer` goroutine still had to write to a full channel (which is a blocking operation).

As a solution, the `TaskProducer` is now the only routine listening on the `context`; all other routines depend only on their respective read-channels, and finalize their work in a cascade fashion when there is nothing left to read on their channels.

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
