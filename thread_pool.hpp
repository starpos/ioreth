/**
 * thread_pool.hpp - a simple fixed thread pool for c++11.
 * @author HOSHINO Takashi
 */
#ifndef THREAD_POOL
#define THREAD_POOL

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <iterator>
#include <list>
#include <map>
#include <queue>
#include <functional>
#include <algorithm>
#include <memory>
#include <cstdio>
#include <cassert>

/**
 * A simple fixed-sized thread pool.
 * type T is of items which will be passed to worker threads.
 */
namespace thread_pool {

class ShouldStopException : public std::exception {};

template<typename T>
class ThreadPoolBase
{
protected:
    const unsigned int poolSize_;
    const unsigned int queueSize_;

    std::mutex mutex_;
    std::condition_variable cvEmpty_; // wait for empty -> not empty.
    std::condition_variable cvFull_;  // wait for full -> not full.
    std::condition_variable cvFlush_; // wait for not empty -> empty.
    bool shouldStop_; // protected by the mutex.
    std::queue<T> waitQ_; // protected by the mutex.

    std::vector<std::thread> workers_;

    std::atomic<bool> canSubmit_;
    std::once_flag joinFlag_;

public:
    /**
     * Constructor.
     * @poolSize Number of worker threads.
     * @queueSize Maximum length of queue.
     */
    ThreadPoolBase(unsigned int poolSize, unsigned int queueSize)
        : poolSize_(poolSize)
        , queueSize_(queueSize)
        , shouldStop_(false)
        , canSubmit_(true)  {}
    
    virtual ~ThreadPoolBase() throw() {

        stop();
        join();
    }

    /**
     * Submit a task.
     * RETURN:
     * true in success, or false.
     */
    bool submit(T task) {

        if (canSubmit_.load()) {
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

        canSubmit_.store(false);
        std::unique_lock<std::mutex> lk(mutex_);
        while (!waitQ_.empty() && !shouldStop_) {
            cvFlush_.wait(lk);
        }
        canSubmit_.store(true);
        return !shouldStop_;
    }

    /**
     * Stop all threads as soon as possible.
     */
    void stop() {

        std::unique_lock<std::mutex> lk(mutex_);
        shouldStop_ = true;
        cvEmpty_.notify_all();
        cvFull_.notify_all();
        cvFlush_.notify_all();
    }

    /**
     * Join threads.
     */
    void join() {

        std::call_once(joinFlag_, [&]() {
                std::for_each(workers_.begin(), workers_.end(), [](std::thread& th) {
                        th.join();
                    });
            });
    }

protected:

    void init(const std::function<void()>& do_work) {

        for (unsigned int i = 0; i < poolSize_; i++) {

            std::thread th(do_work);
            workers_.push_back(std::move(th));
        }
    }
    
    bool enqueue(T task) {

        std::unique_lock<std::mutex> lk(mutex_);

        while (waitQ_.size() >= queueSize_ && !shouldStop_) {
            cvFull_.wait(lk);
        }
        if (shouldStop_) {
            return false;
        } else {
            cvEmpty_.notify_one();
            waitQ_.push(task);
            return true;
        }
    }

    T dequeue() {

        std::unique_lock<std::mutex> lk(mutex_);

        while (waitQ_.empty() && !shouldStop_) {
            cvEmpty_.wait(lk);
        }
        if (shouldStop_) {
            throw ShouldStopException();
        } else {
            cvFull_.notify_one();
            T task = waitQ_.front();
            waitQ_.pop();
            if (waitQ_.empty()) { cvFlush_.notify_all(); }
            return task;
        }
    }
};

} // namespace thread_pool

/**
 * Simple thread pool with tasks of type T and
 * worker function of type std::function<void(T)>.
 *
 * Currently worker function could not throw exceptions.
 * Use ThreadPoolWithId instead.
 */
template<typename T>
class ThreadPool : public thread_pool::ThreadPoolBase<T>
{
private:
    typedef thread_pool::ThreadPoolBase<T> TPB;
    std::function<void(T)> workerFunc_;

public:
    ThreadPool(unsigned int poolSize, unsigned int queueSize,
               const std::function<void(T)>& workerFunc)
        : TPB(poolSize, queueSize)
        , workerFunc_(workerFunc) {

        TPB::init([&] { this->do_work(); });
    }
    
    ~ThreadPool() throw() {}

private:
    void do_work() throw() {

        while (!TPB::shouldStop_) {
            try {
                workerFunc_(TPB::dequeue());
            } catch (thread_pool::ShouldStopException& e) {
                break;
            }
        }
    }
};


/**
 * Simple thread pool with thread id and promise data.
 * Wroker function can throw an exception and you can get it by get().
 */
template<typename T>
class ThreadPoolWithId : public thread_pool::ThreadPoolBase<T>
{
private:
    typedef thread_pool::ThreadPoolBase<T> TPB;

    std::mutex mutex_;
    std::condition_variable cv_;
    bool isInitialized_;

    /* The second is thread id starting from 0. */
    std::function<void(T, unsigned int)> workerFuncWithId_;

    std::map<std::thread::id, unsigned int> idMap_;
    std::vector<std::promise<void> > promises_;
    std::vector<std::future<void> > futures_;
    
public:
    ThreadPoolWithId(unsigned int poolSize, unsigned int queueSize,
                     const std::function<void(T, unsigned int)>& workerFuncWithId)
        : TPB(poolSize, queueSize)
        , isInitialized_(false)
        , workerFuncWithId_(workerFuncWithId)
        , promises_(poolSize)
        , futures_(poolSize) {

        TPB::init([&] { this->do_work(); });
        for (unsigned int i = 0; i < poolSize; i++) {
            auto tid = TPB::workers_[i].get_id();
            idMap_[tid] = i;
            futures_[i] = std::move(promises_[i].get_future());
        }
        {
            std::unique_lock<std::mutex> lk(mutex_);
            isInitialized_ = true;
            cv_.notify_all();
        }
    }

    ~ThreadPoolWithId() throw() {
        
        TPB::stop();
        TPB::join();
        
        bool canThrow = false;
        getDetail(canThrow);
    }

    /**
     * Wait for all threads end.
     * An exception will be thrown.
     * You can call this mutliple times to get multiple exceptions.
     * You need call this 'poolSize' times at most to get all exceptions.
     */
    void get() {

        bool canThrow = true;
        getDetail(canThrow);
    }

    /**
     * Wait until a time point or all threads done.
     */
    void waitUntil(const std::chrono::steady_clock::time_point& time) {

        std::for_each(futures_.begin(), futures_.end(), [&] (std::future<void>& f) {
                f.wait_until(time);
            });
    }

    /**
     * Wait for a timeout period or all threads done.
     */
    void waitFor(const std::chrono::steady_clock::duration& period) {

        auto time = std::chrono::steady_clock::now() + period;
        std::for_each(futures_.begin(), futures_.end(), [&] (std::future<void>& f) {
                f.wait_until(time);
            });
    }

private:
    void do_work() throw() {

        auto tid = std::this_thread::get_id();
        {
            /* Wait for all threads created. */
            std::unique_lock<std::mutex> lk(mutex_);
            while (!isInitialized_) {
                cv_.wait(lk);
            }
        }
        /* Now idMap_, promises_, and futures_ is filled. */
        unsigned int id = idMap_[tid];

        try {
            while (!TPB::shouldStop_) {
                try {
                    workerFuncWithId_(TPB::dequeue(), id);
                    
                } catch (thread_pool::ShouldStopException& e) {
                    break;
                }
            }
            promises_[id].set_value();
        } catch (...) {
            promises_[id].set_exception(std::current_exception());
            TPB::stop();
        }
    }

    void getDetail(bool canThrow) {
        
        std::for_each(futures_.begin(), futures_.end(), [&] (std::future<void>& f) {
                if (f.valid()) {
                    try {
                        f.get();
                    } catch (...) {
                        if (canThrow) {
                            std::rethrow_exception(std::current_exception());
                        }
                    }
                }
            });
    }
};


namespace thread_pool {

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
    TaskBase(const TaskBase&) = delete;
    TaskBase(TaskBase&&) = delete;
    TaskBase& operator=(const TaskBase&) = delete;
    TaskBase& operator=(TaskBase&&) = delete;
    bool valid() const { return future_.valid(); }
};

} // namespace thread_pool


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
class Task : public thread_pool::TaskBase<T1, T2>
{
private:
    typedef thread_pool::TaskBase<T1, T2> TB;
    std::function<T2(T1)> func_;
    T1 arg_;
public:
    Task(const std::function<T2(T1)>& func, T1 arg, std::promise<T2>&& promise)
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
class Task<T1, void> : public thread_pool::TaskBase<T1, void>
{
private:
    typedef thread_pool::TaskBase<T1, void> TB;
    std::function<void(T1)> func_;
    T1 arg_;
public:
    Task(const std::function<void(T1)>& func, T1 arg, std::promise<void>&& promise)
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
class Task<void, T2> : public thread_pool::TaskBase<void, T2>
{
private:
    typedef thread_pool::TaskBase<void, T2> TB;
    std::function<T2()> func_;
public:
    Task(const std::function<T2()>& func, std::promise<T2>&& promise)
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
class Task<void, void> : public thread_pool::TaskBase<void, void>
{
private:
    typedef thread_pool::TaskBase<void, void> TB;
    std::function<void()> func_;
public:
    Task(const std::function<void()>& func, std::promise<void>&& promise)
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
