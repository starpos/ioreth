/**
 * Test code for thread_pool.hpp header.
 */
#include "thread_pool.hpp"

#include <cstdio>
#include <future>
#include <memory>
#include <functional>
#include <chrono>
#include <atomic>

/**
 * Thread pool test with Task class.
 */
void testThreadPoolWithTask()
{
    typedef std::shared_ptr<Task<int, int> > TaskPtr;
    std::function<int(int)> f([](int a) {
            printf("f(%d) called.\n", a);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            return -a;
        });
    
    std::function<void(TaskPtr)> workerFunc([](TaskPtr task) { task->run(); });
    ThreadPool<TaskPtr> threadPool(4, 16, workerFunc);

    std::list<TaskPtr> li;
    std::thread submitter([&]() {
            for (int i = 0; i < 20; i ++) {

                std::promise<int> p;
                auto taskPtr = TaskPtr(new Task<int, int>(f, i, std::move(p)));
                if (threadPool.submit(taskPtr)) {
                    li.push_back(taskPtr);
                } else {
                    break;
                }
            }
        });
    std::thread finalizer([&]() {

            while (li.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            
            std::for_each(li.begin(), li.end(), [](TaskPtr task) {
                    printf("result %d\n", task->get());
                });
        });

    submitter.join();
    threadPool.flush();
    threadPool.stop();
    threadPool.join();
    finalizer.join();
}

size_t testThreadPoolOverhead(
    int nEnqueThreads, int nDequeueThreads, int workQueueSize, int runPeriodMs)
{
    std::atomic<size_t> count(0);

    std::function<void(int)> counter([&](int i) {
            size_t s;
            do {
                s = count;
            } while(!count.compare_exchange_strong(s, s + 1));
        });

    ThreadPool<int> threadPool(nDequeueThreads, workQueueSize, counter);

    std::vector<std::thread> workers;
    std::atomic<bool> shouldStop(false);
    for (int i = 0; i < nEnqueThreads; i ++) {

        std::thread th([&]{
                while (!shouldStop.load()) {
                    threadPool.submit(0);
                }
            });
        workers.push_back(std::move(th));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(runPeriodMs));
    shouldStop.store(true);

    std::for_each(workers.begin(), workers.end(), [](std::thread& th) {
            th.join();
        });

    threadPool.flush();
    size_t total = count;
    return total;
}

void testThreadPoolWithId()
{

    std::function<void(int, unsigned int)> f([](int i, unsigned int id) {

            printf("Thread %u working with item %d.\n", id, i);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        });
    
    ThreadPoolWithId<int> threadPool(4, 16, f);

    for (int i = 0; i < 20; i ++) {
        threadPool.submit(i);
    }

    threadPool.flush();
}


void testCounterWithCas(int nThreads, size_t runPeriod)
{
    // Performance of counter with CAS.
    std::atomic<size_t> count(0);
    std::atomic<bool> shouldStop(false);
    std::vector<std::thread> threads;
    threads.reserve(nThreads);
    threads.push_back(std::move(std::thread([&] {
                    size_t s;
                    while (!shouldStop.load()) {
                        s = count;
                        count.compare_exchange_strong(s, s + 1);
                    }
                })));
    std::this_thread::sleep_for(std::chrono::seconds(runPeriod));
    shouldStop.store(true);
    
    std::for_each(threads.begin(), threads.end(), [](std::thread& th) {
            th.join();
        });
    size_t s = count;
    printf("testCounterWithCas %zu in %zu secs with %d threads (%zu num/s)\n", s, runPeriod, nThreads, s / runPeriod);
        
}

/**
 * Performance with a normal counter.
 */
void testCounter(size_t runPeriod)
{
    std::atomic<bool> shouldStop(false);
    size_t s = 0;
    std::thread th([&] {
            while (!shouldStop.load()) {
                s ++;
            }
        });
    std::this_thread::sleep_for(std::chrono::seconds(runPeriod));
    shouldStop.store(true);
    th.join();
    printf("testCounter %zu in %zu secs (%zu num/s)\n", s, runPeriod, s / runPeriod);
}


int main()
{
#if 0
    {
        testCounter(3);
        testCounterWithCas(1, 3);
        testCounterWithCas(2, 3);
        testCounterWithCas(3, 3);
        testCounterWithCas(4, 3);
    }
    {
        testThreadPoolWithTask();
    }
#endif
#if 1
    {
        for (int i = 0; i < 1; i ++) {
            for (int j = 0; j < 4; j ++) {
                for (int k = 0; k < 4; k ++) {
                    size_t nEnq = i + 1;
                    size_t nDeq = j + 1;
                    int queueSize = (k + 1) * 8;
                    printf("%zu %zu %2d %10zu\n", nEnq, nDeq, queueSize,
                           testThreadPoolOverhead(nEnq, nDeq, queueSize, 1000));
                }
            }
        }
    }
#endif
#if 0
    {
        int nEnq = 1;
        int nDeq = 1;
        int queueSize = 16;
        int runPeriodMs = 5000;
        size_t count = testThreadPoolOverhead(nEnq, nDeq, queueSize, runPeriodMs);
        printf("nEnq %d nDeq %d qSize %2d count %10zu ops %10zu\n",
               nEnq, nDeq, queueSize, count, count / (runPeriodMs / 1000));
    }
    {
        testThreadPoolWithId();
    }
#endif
    return 0;
}
