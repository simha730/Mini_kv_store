# Unix Domain Socket Key-Value Store Performance Report

## Performance Summary

* **Test duration:** 106 seconds under concurrent load
* **Syscalls observed (kvstore_server_mt.c ):**

  * `read`  : 6
  * `write` : 4
  * `accept`: 2

* **Test duration:** 177 seconds under concurrent load
* **Syscalls observed (kvstore_client.c):**

  * `read`  : 6
  * `write` : 11
  * `accept`: 0

* **Observations:**

  * A lot of `read` and `write` calls show the server spends most of its time on **I/O**.
  * `accept` shows how many **clients connected** to the server.
  * When many clients connect at the same time, threads may **wait on locks**, which can slow down writes.

## Load Scenarios

### 1. Read-heavy Load

* **State:** Many clients performing `GET` operations simultaneously.
* **Observation:** Server handles reads quickly; very little waiting.
* **Fix / Optimization:** No major fix needed; consider caching if read volume grows further.

### 2. Write-heavy Load

* **State:** Many clients performing `SET` operations.
* **Observation:** Threads sometimes wait on locks `mutex`, slowing writes.
* **Fix / Optimization:** Use smaller locks or lock-free structures to reduce waiting.

### 3. Mixed Load

* **State:** Clients do both `GET` and `SET`.
* **Observation:** Some delays from locks; moderate speed.
* **Fix / Optimization:** Optimize critical sections; minimize lock duration.

### 4. High Concurrency

* **State:** Many clients connecting simultaneously.
* **Observation:** The server makes a lot of accept calls and threads wait on locks, which can slow things down.
* **Fix / Optimization:** Consider using a **thread pool** instead of one thread per client, or use **asynchronous I/O** to handle many clients efficiently without blocking.

## Commands Used

### 1. Compile the server/client

gcc kvstore_server_mt.c -o kvstore_server_mt
gcc kvstore_client.c -o kvstore_client


### 2. Run the server
./kvstore_server_mt

### 3. Run multiple clients manually
./kvstore_client   # Terminal 1
../kvstore_client   # Terminal 2

### 4. Profile the server/client with `perf`
sudo perf stat -e syscalls:sys_enter_read,syscalls:sys_enter_write,syscalls:sys_enter_accept ./kvstore_server_mt
sudo perf stat -e syscalls:sys_enter_read,syscalls:sys_enter_write,syscalls:sys_enter_accept ./kvstore_client

### 5. (Optional) View server PID manually
ps aux | grep kvstore_server_mt
### 6. Capture screenshot 
* WSL: Windows + Shift + S
* Linux GUI users: PrtScn or gnome-screenshot -a

## Notes

* This report **excludes server/client code**.
* Designed to show **system behavior under different loads** and **bottleneck analysis**.
* Provides insights for **optimizing thread-based key-value store** performance.

