#!/usr/bin/env bash

#
# basic map-reduce test
#

#RACE=

# comment this to run the tests without the Go race detector.
RACE=-race

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd ../coordinator && go build $RACE mrcoordinator.go) || exit 1
(cd ../worker && go build $RACE mrworker.go) || exit 1
(cd ../mrsequential && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
cd ../mrsequential && (./mrsequential ../mrapps/wc.so ../map_data/pg*txt || exit 1)
sort mr-out-0 > ../mr-correct-wc.txt
rm -f mr-out*

pwd

echo '***' Starting wc test.

cd ..

timeout -k 2s 180s coordinator/mrcoordinator map_data/pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s worker/mrworker mrapps/wc.so &
timeout -k 2s 180s worker/mrworker mrapps/wc.so &
timeout -k 2s 180s worker/mrworker mrapps/wc.so &

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

rm -f mr-out*

# wait for remaining workers and coordinator to exit.
wait

#########################################################
# now indexer
rm -f mr-*

# generate the correct output
mrsequential/mrsequential mrapps/indexer.so map_data/pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

timeout -k 2s 180s coordinator/mrcoordinator map_data/pg*txt &
pid=$!

sleep 1

# start multiple workers
timeout -k 2s 180s worker/mrworker mrapps/indexer.so &
timeout -k 2s 180s worker/mrworker mrapps/indexer.so &

wait $pid

pwd

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

rm -f mr-out*

wait

#########################################################
echo '***' Starting crash test.

# generate the correct output
mrsequential/mrsequential mrapps/nocrash.so map_data/pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
(timeout -k 2s 180s coordinator/mrcoordinator map_data/pg*txt ; touch mr-done ) &
sleep 1

# start multiple workers
timeout -k 2s 180s worker/mrworker mrapps/crash.so &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/824-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    timeout -k 2s 180s worker/mrworker mrapps/crash.so 
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    timeout -k 2s 180s worker/mrworker mrapps/crash.so 
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  timeout -k 2s 180s worker/mrworker mrapps/crash.so
  sleep 1
done

wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

rm -f mr-*