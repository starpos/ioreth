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
namespace __thread_pool {

class ShouldStopException : public std::exception {};

template<typename T>
class ThreadPoolBase
{
protected:
    volatile bool shouldStop_;
    volatile bool canSubmit_;
    
    const unsigned int poolSize_;
    const unsigned int queueSize_;

    std::mutex mutex_;
    std::condition_variable cv_;

    std::queue<T> waitQ_;
    std::vector<std::thread> workers_;

    std::once_flag joinFlag_;

public:
    /**
     * Constructor.
     * @poolSize Number of worker threads.
     * @queueSize Maximum length of queue.
     */
    ThreadPoolBase(unsigned int poolSize, unsigned int queueSize)
        : shouldStop_(false)
        , canSubmit_(true)
        , poolSize_(poolSize)
        , queueSize_(queueSize) {}
    
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

protected:

    void init(const std::function<void()>& do_work) {

        for (unsigned int i = 0; i < poolSize_; i ++) {

            std::thread th(do_work);
            workers_.push_back(std::move(th));
        }
    }
    
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
};

} // namespace __thread_pool

template<typename T>
class ThreadPool : public __thread_pool::ThreadPoolBase<T>
{
private:
    typedef __thread_pool::ThreadPoolBase<T> TPB;
    std::function<void(T)> workerFunc_;
public:
    ThreadPool(unsigned int poolSize, unsigned int queueSize,
                        const std::function<void(T)>& workerFunc)
        : TPB(poolSize, queueSize)
        , workerFunc_(workerFunc) {

        TPB::init([&] { this->do_work(); });
    }
    
    ~ThreadPool() throw() {}

    void do_work() throw() {
         
         while (!TPB::shouldStop_) {
            
            try {
                workerFunc_(TPB::dequeue());
                
            } catch (__thread_pool::ShouldStopException& e) {
                break;
            }
        }
    }
};

template<typename T>
class ThreadPoolWithId : public __thread_pool::ThreadPoolBase<T>
{
private:
    typedef __thread_pool::ThreadPoolBase<T> TPB;

    std::mutex mutex_;

    std::map<std::thread::id, unsigned int> idMap_;
    /* The second is thread id starting from 0. */
    std::function<void(T, unsigned int)> workerFuncWithId_;
public:
    ThreadPoolWithId(unsigned int poolSize, unsigned int queueSize,
                     const std::function<void(T, unsigned int)>& workerFuncWithId)
        : TPB(poolSize, queueSize)
        , workerFuncWithId_(workerFuncWithId) {

        TPB::init([&] { this->do_work(); });
        for (unsigned int i = 0; i < poolSize; i ++) {
            idMap_[TPB::workers_[i].get_id()] = i;
        }
    }

    ~ThreadPoolWithId() throw() {}

    void do_work() throw() {
        
        while (!TPB::shouldStop_) {
            try {
                workerFuncWithId_(TPB::dequeue(), idMap_[std::this_thread::get_id()]);
                
            } catch (__thread_pool::ShouldStopException& e) {
                break;
            }
        }
    }

#if 0
    void check(const std::string& s) {

        std::for_each(idMap_.begin(), idMap_.end(), [&](const std::pair<std::thread::id, unsigned int>& p) {

                    std::lock_guard<std::mutex> lk(mutex_);
                    std::cout << s << ": " << p.first << " i: " << p.second << std::endl;
            });
    }
#endif

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

} // namespace __thread_pool


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
class Task<T1, void> : public __thread_pool::TaskBase<T1, void>
{
private:
    typedef __thread_pool::TaskBase<T1, void> TB;
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
class Task<void, T2> : public __thread_pool::TaskBase<void, T2>
{
private:
    typedef __thread_pool::TaskBase<void, T2> TB;
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
class Task<void, void> : public __thread_pool::TaskBase<void, void>
{
private:
    typedef __thread_pool::TaskBase<void, void> TB;
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
