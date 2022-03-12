#h1 MapReduce implementation

Lab01 - 6824 MIT

Implement a worker process that calls application Map and Reduce functions and handles reading and writing files, and a coordinator process that hands out tasks to workers and copes with failed workers.

Processes comunicate via UNIX domain socket

Get Map, Reduce functions through plugin (built by go build -buildmode=plugin)
