#!/bin/bash

#export VERBOSE=$1
export VERBOSE=1

go build -race -buildmode=plugin -o /Users/archer/project/6.5840/build/bin/plugin.so /Users/archer/project/6.5840/src/mrapps/wc.go

echo "build plugin successfully!"

go build -race -o /Users/archer/project/6.5840/build/bin/worker /Users/archer/project/6.5840/src/main/mrworker.go

echo "build worker successfully!"

#for ((i = 1; i <= $2; i++)); do
#	nohup /Users/archer/project/6.5840/build/bin/worker /Users/archer/project/6.5840/build/bin/plugin.so >/Users/archer/project/6.5840/build/logs/worker-$i.logs 2>&1 &
#done


# 定义一个数组用于保存worker进程的PID
worker_pids=()

# 定义捕捉Ctrl+C信号的函数
trap 'kill_workers; exit' INT

kill_workers() {
    echo "Caught Ctrl+C, killing worker processes..."

    # 使用循环遍历数组中的PID并逐个杀死
    for pid in "${worker_pids[@]}"; do
        kill $pid
    done

    echo "Worker processes killed."
}

# 启动worker进程并记录PID
for ((i = 1; i <= 4; i++)); do
    nohup /Users/archer/project/6.5840/build/bin/worker /Users/archer/project/6.5840/build/bin/plugin.so >/Users/archer/project/6.5840/build/logs/worker-$i.log 2>&1 &

    # 记录每个worker进程的PID
    worker_pids+=($!)
done

# 在脚本的末尾，等待后台任务完成
wait

cat mr-out-* | sort | more


