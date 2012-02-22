/**
 * thread_pool.hpp - a simple fixed thread pool for c++11.
 * @author HOSHINO Takashi
 */
#ifndef THREAD_POOL
#define THREAD_POOL

#include <thread>
#include <future>
#include <list>
#include <queue>
#include <functional>
#include <algorithm>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <cstdio>

/**
 * A simple fixed-sized thread pool.
 * type T is of items which will be passed to worker threads.
 */
template<typename T>
class ThreadPool
{
private:
    volatile bool shouldStop_;
    volatile bool canSubmit_;
    
    const size_t poolSize_;
    const size_t queueSize_;

    std::mutex mutex_;
    std::condition_variable cv_;

    std::queue<T> waitQ_;
    std::vector<std::thread> workers_;

    std::once_flag joinFlag_;

    std::function<void(T)> workerFunc_;

public:
    /**
     * Constructor.
     * @poolSize Number of worker threads.
     * @queueSize Maximum length of queue.
     * @workerFunc A function that will be executed by worker threads.
     */
    explicit ThreadPool(size_t poolSize, size_t queueSize,
                        const std::function<void(T)>& workerFunc)
        : shouldStop_(false)
        , canSubmit_(true)
        , poolSize_(poolSize)
        , queueSize_(queueSize)
        , workerFunc_(workerFunc) {

        for (size_t i = 0; i < poolSize_; i ++) {

            std::thread th([&]() { this->do_work(); });
            workers_.push_back(std::move(th));
        }
    }

    ~ThreadPool() throw() {

        stop();
        join();
    }

    /**
     * Submit a task.
     * RETURN:
     * true in success, or false.
     */
    bool submit(T task) {

        if (canSubmit_) {
            return enqueue(task);
        } else {
            return false;
        }
    }

    /**
     * Flush all tasks pending/running.
     * submit() call is prehibited during flushing.
     *
     * RETURN:
     * false when join() is called dring flushing.
     */
    bool flush() {

        canSubmit_ = false;
        std::unique_lock<std::mutex> lk(mutex_);
        while (!waitQ_.empty() && !shouldStop_) {
            cv_.wait(lk);
        }
        if (shouldStop_) { return false; }
        canSubmit_ = true;
        cv_.notify_all();
        return true;
    }

    /**
     * Stop all threads as soon as possible.
     */
    void stop() {

        shouldStop_ = true;
        cv_.notify_all();
    }

    /**
     * Join threads.
     */
    void join() {

        std::call_once(joinFlag_, [&]() {
                std::for_each(workers_.begin(), workers_.end(), [](std::thread& th) {
                        th.join();
                    }); });
    }

private:

    struct ShouldStopException : public std::exception {};
    
    bool enqueue(T task) {

        std::unique_lock<std::mutex> lk(mutex_);

        while (waitQ_.size() >= queueSize_ && !shouldStop_) {
            cv_.wait(lk);
        }
        if (shouldStop_) {
            return false;
        }
        waitQ_.push(task);
        cv_.notify_all();
        return true;
    }

    T dequeue() {

        std::unique_lock<std::mutex> lk(mutex_);

        while (waitQ_.empty() && !shouldStop_) {
            cv_.wait(lk);
        }
        if (shouldStop_) {
            throw ShouldStopException();
        }
        T task = waitQ_.front();
        waitQ_.pop();
        cv_.notify_all();
        return task;
    }

    void do_work() {

#if 0
        printf("do_worker start.\n");
#endif
        while (!shouldStop_) {

            try {
                workerFunc_(dequeue());
                
            } catch (ShouldStopException& e) {
                break;
            }
        }
#if 0
        printf("do_worker end.\n");
#endif
    }
};

namespace __thread_pool {

/**
 * Base class for Task to run by a thread in a thread pool.
 *
 * T1 is argument type.
 * T2 is return value type.
 */
template<typename T1, typename T2>
class TaskBase
{
protected:
    std::promise<T2> promise_;
    std::shared_future<T2> future_;
    
public:
    explicit TaskBase(std::promise<T2>&& promise)
        : promise_(std::move(promise))
        , future_(promise_.get_future()) {}
    virtual ~TaskBase() throw() {}
    bool valid() const { return future_.valid(); }
private:
    TaskBase& operator=(const TaskBase& task) { return *this; }
    TaskBase& operator=(TaskBase&& task) { return *this; }
    TaskBase(const TaskBase& task) {}
};

}

/**
 * Task class with normal type T1 and T2.
 *
 * Prepare a function of std::function<T2(T1)> type,
 * an argument of T1 type, and an std::promise<T2> object.
 * Then create an Task instance with them.
 * 
 * You can run task with run().
 * You can get result by get().
 * Do not call promise.get_future() to construct an instance.
 */
template<typename T1, typename T2>
class Task : public __thread_pool::TaskBase<T1, T2>
{
private:
    typedef __thread_pool::TaskBase<T1, T2> TB;
    std::function<T2(T1)> func_;
    T1 arg_;
public:
    explicit Task(const std::function<T2(T1)>& func, T1 arg, std::promise<T2>&& promise)
        : TB(std::move(promise))
        , func_(func)
        , arg_(arg) {}
    ~Task() throw() {}
    void run() {
        try {
            TB::promise_.set_value(std::move(func_(std::ref(arg_))));
        } catch (...) {
            TB::promise_.set_exception(std::current_exception());
        }
    }
    const T2& get() { return TB::future_.get(); }
};

/**
 * Task class with T1, void.
 */
template<typename T1>
class Task<T1, void> : public __thread_pool::TaskBase<T1, void>
{
private:
    typedef __thread_pool::TaskBase<T1, void> TB;
    std::function<void(T1)> func_;
    T1 arg_;
public:
    explicit Task(const std::function<void(T1)>& func, T1 arg, std::promise<void>&& promise)
        : TB(std::move(promise))
        , func_(func)
        , arg_(arg) {}
    ~Task() throw() {}
    void run() {
        try {
            func_(std::ref(arg_));
            TB::promise_.set_value();
        } catch (...) {
            TB::promise_.set_exception(std::current_exception());
        }
    }
    void get() { TB::future_.get(); }
};

/**
 * Task class with void, T2.
 */
template<typename T2>
class Task<void, T2> : public __thread_pool::TaskBase<void, T2>
{
private:
    typedef __thread_pool::TaskBase<void, T2> TB;
    std::function<T2()> func_;
public:
    explicit Task(const std::function<T2()>& func, std::promise<T2>&& promise)
        : TB(std::move(promise))
        , func_(func) {}
    ~Task() throw() {}
    void run() {
        try {
            TB::promise_.set_value(std::move(func_()));
        } catch (...) {
            TB::promise_.set_exception(std::current_exception());
        }
    }
    const T2& get() { return TB::future_.get(); }
};

/**
 * Task class with void, void.
 */
template<>
class Task<void, void> : public __thread_pool::TaskBase<void, void>
{
private:
    typedef __thread_pool::TaskBase<void, void> TB;
    std::function<void()> func_;
public:
    explicit Task(const std::function<void()>& func, std::promise<void>&& promise)
        : TB(std::move(promise))
        , func_(func) {}
    ~Task() throw() {}
    void run() {
        try {
            func_();
            TB::promise_.set_value();
        } catch (...) {
            TB::promise_.set_exception(std::current_exception());
        }
    }
    void get() { TB::future_.get(); }
};


#endif /* THREAD_POOL */
