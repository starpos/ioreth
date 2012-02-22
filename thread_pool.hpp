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
 * Base class for Task to run by a thread in a thread pool.
 *
 * T1 is argument type.
 * T2 is return value type.
 */
template<typename T1, typename T2>
class __TaskBase
{
protected:
    std::promise<T2> promise_;
    std::shared_future<T2> future_;
    
public:
    explicit __TaskBase(std::promise<T2>&& promise)
        : promise_(std::move(promise))
        , future_(promise_.get_future()) {}
    virtual ~__TaskBase() throw() {}
    bool valid() const { return future_.valid(); }
private:
    __TaskBase& operator=(const __TaskBase& task) { return *this; }
    __TaskBase& operator=(__TaskBase&& task) { return *this; }
    __TaskBase(const __TaskBase& task) {}
};

/**
 * Task class with normal type T1 and T2.
 */
template<typename T1, typename T2>
class Task : public __TaskBase<T1, T2>
{
private:
    typedef __TaskBase<T1, T2> TB;
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
class Task<T1, void> : public __TaskBase<T1, void>
{
private:
    typedef __TaskBase<T1, void> TB;
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
class Task<void, T2> : public __TaskBase<void, T2>
{
private:
    typedef __TaskBase<void, T2> TB;
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
class Task<void, void> : public __TaskBase<void, void>
{
private:
    typedef __TaskBase<void, void> TB;
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


template<typename T1, typename T2>
class ThreadPool
{
    typedef std::shared_ptr<Task<T1, T2> > TaskPtr;

private:
    volatile bool shouldStop_;
    volatile bool canSubmit_;
    
    const size_t poolSize_;
    const size_t queueSize_;

    std::mutex mutex_;
    std::condition_variable cv_;

    std::queue<TaskPtr> waitQ_;
    std::vector<std::thread> workers_;

    std::once_flag joinFlag_;

public:
    explicit ThreadPool(size_t poolSize, size_t queueSize)
        : shouldStop_(false)
        , canSubmit_(true)
        , poolSize_(poolSize)
        , queueSize_(queueSize) {

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
    bool submit(TaskPtr task) {

        if (canSubmit_) {
            return enqueueTask(task);
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

    TaskPtr dequeueTask() {

        std::unique_lock<std::mutex> lk(mutex_);

        while (waitQ_.empty() && !shouldStop_) {
            cv_.wait(lk);
        }
        if (shouldStop_) {
            return TaskPtr(); // nullptr.
        }
        TaskPtr task = waitQ_.front();
        waitQ_.pop();
        cv_.notify_all();
        return task;
    }

    bool enqueueTask(TaskPtr task) {

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

    void do_work() {

#if 0
        printf("do_worker start.\n");
#endif
        while (!shouldStop_) {

            TaskPtr task = dequeueTask();
            if (!task) { break; }
            task->run();
        }
#if 0
        printf("do_worker end.\n");
#endif
    }
};

#endif /* THREAD_POOL */
