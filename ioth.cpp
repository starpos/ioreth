/**
 * @file
 * @brief To know IO throughput of file or device.
 * @author HOSHINO Takashi
 */
#define _FILE_OFFSET_BITS 64

#include <iostream>
#include <vector>
#include <string>
#include <list>
#include <sstream>
#include <queue>
#include <utility>
#include <tuple>
#include <algorithm>
#include <future>
#include <mutex>
#include <memory>
#include <chrono>

#include <cstdio>
#include <cassert>
#include <cstring>
#include <cstdlib>

#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "ioreth.hpp"
#include "util.hpp"
#include "thread_pool.hpp"

/**
 * Parse commane-line arguments as options.
 */
class Options
{
private:
    std::string programName_;
    size_t startBlockId_;
    size_t blockSize_;
    std::vector<std::string> args_;
    Mode mode_;
    bool isShowEachResponse_;
    bool isShowVersion_;
    bool isShowHelp_;
    
    size_t period_;
    size_t count_;
    size_t nthreads_;
    size_t queueSize_;

public:
    Options(int argc, char* argv[])
        : startBlockId_(0)
        , blockSize_(0)
        , args_()
        , mode_(READ_MODE)
        , isShowEachResponse_(false)
        , isShowVersion_(false)
        , isShowHelp_(false)
        , period_(0)
        , count_(0)
        , nthreads_(1)
        , queueSize_(1) {

        parse(argc, argv);

        if (isShowVersion_ || isShowHelp_) {
            return;
        }
        checkAndThrow();
    }

    void showVersion() {

        ::printf("iores version %s\n", IORETH_VERSION);
    }
    
    void showHelp() {

        ::printf("usage: %s [option(s)] [file or device]\n"
                 "options: \n"
                 "    -s off:  start offset in blocks.\n"
                 "    -b size: blocksize in bytes.\n"
                 "    -p secs: execute period in seconds.\n"
                 "    -c num:  number of IOs to execute.\n"
                 "             -p and -c is exclusive.\n"
                 "    -w:      write instead read.\n"
                 "    -t num:  number of threads in parallel.\n"
                 "             if 0, use aio instead thread.\n"
                 "    -q size: queue size.\n"
                 "    -r:      show response of each IO.\n"
                 "    -v:      show version.\n"
                 "    -h:      show this help.\n"
                 , programName_.c_str()
            );
    }

    const std::vector<std::string>& getArgs() const { return args_; }
    size_t getStartBlockId() const { return startBlockId_; }
    size_t getBlockSize() const { return blockSize_; }
    Mode getMode() const { return mode_; }
    bool isShowEachResponse() const { return isShowEachResponse_; }
    bool isShowVersion() const { return isShowVersion_; }
    bool isShowHelp() const { return isShowHelp_; }
    size_t getPeriod() const { return period_; }
    size_t getCount() const { return count_; }
    size_t getNthreads() const { return nthreads_; }
    size_t getQueueSize() const { return queueSize_; }

private:
    void parse(int argc, char* argv[]) {

        programName_ = argv[0];
        
        while (1) {
            int c = ::getopt(argc, argv, "s:b:p:c:t:q:wrvh");

            if (c < 0) { break; }

            switch (c) {
            case 's': /* start offset in blocks */
                startBlockId_ = ::atol(optarg);
                break;
            case 'b': /* blocksize */
                blockSize_ = ::atol(optarg);
                break;
            case 'p': /* period */
                period_ = ::atol(optarg);
                break;
            case 'c': /* count */
                count_ = ::atol(optarg);
                break;
            case 'w': /* write */
                mode_ = WRITE_MODE;
                break;
            case 't': /* nthreads */
                nthreads_ = ::atol(optarg);
                break;
            case 'q': /* queueSize */
                queueSize_ = ::atol(optarg);
                break;
            case 'r': /* show each response */
                isShowEachResponse_ = true;
                break;
            case 'v': /* show version */
                isShowVersion_ = true;
                break;
            case 'h': /* help */
                isShowHelp_ = true;
                break;
            }
        }

        while (optind < argc) {
            args_.push_back(argv[optind++]);
        }
    }

    void checkAndThrow() {

        if (args_.size() != 1 || blockSize_ == 0) {
            throw std::runtime_error("specify blocksize (-b), and device.");
        }
        if (period_ == 0 && count_ == 0) {
            throw std::runtime_error("specify period (-p) or count (-c).");
        }
        if (queueSize_ == 0) {
            throw std::runtime_error("queue size (-q) must be 1 or more.");
        }
    }
};

/**
 * IO throughptu benchmark.
 * This is mutli-threaded.
 */
class IoThroughputBench
{
private:

    const std::string name_;
    const Mode mode_;
    const size_t blockSize_; /* [byte] */
    const unsigned int nThreads_;
    const unsigned queueSize_;
    const bool isShowEachResponse_;
    size_t maxBlockId_;
    
    class ThreadLocalData
    {
    private:
        char *buf_;
        BlockDevice bd_;
        std::queue<IoLog> logQ_;
        size_t blockSize_;
        PerformanceStatistics stat_;

    public:
        ThreadLocalData(BlockDevice&& bd, size_t blockSize)
            : bd_(std::move(bd))
            , blockSize_(blockSize) {

            size_t alignSize = 512;
            while (alignSize < blockSize) {
                alignSize *= 2;
            }
            if(::posix_memalign((void **)&buf_, alignSize, blockSize) != 0) {
                throw std::runtime_error("posix_memalign failed");
            }
        }
        explicit ThreadLocalData(ThreadLocalData&& rhs)
            : buf_(rhs.buf_)
            , bd_(std::move(rhs.bd_))
            , logQ_(std::move(rhs.logQ_))
            , blockSize_(rhs.blockSize_)
            , stat_(rhs.stat_) {

            rhs.buf_ = nullptr;
        }
        ThreadLocalData& operator=(ThreadLocalData&& rhs) {

            buf_ = rhs.buf_; rhs.buf_ = nullptr;
            bd_ = std::move(rhs.bd_);
            logQ_ = std::move(rhs.logQ_);
            blockSize_ = rhs.blockSize_;
            stat_ = rhs.stat_;
            return *this;
        }
        
        ~ThreadLocalData() noexcept { free(buf_); }

        BlockDevice& getBlockDevice() { return bd_; }
        size_t getBlockDeviceSize() const { return bd_.getDeviceSize() / blockSize_; }
        char* getBuffer() { return buf_; }
        std::queue<IoLog>& getLogQueue() { return logQ_; }
        PerformanceStatistics& getPerformanceStatistics() { return stat_; }

    private:
        
    };
    std::vector<ThreadLocalData> threadLocal_;

public:
    /**
     * @param dev block device.
     * @param bs block size.
     * @param nBlocks disk size as number of blocks.
     * @param startBlockId 
     */
    IoThroughputBench(const std::string& name, const Mode mode, size_t blockSize,
                      unsigned int nThreads, unsigned queueSize, bool isShowEachResponse)
        : name_(name)
        , mode_(mode)
        , blockSize_(blockSize)
        , nThreads_(nThreads)
        , queueSize_(queueSize)
        , isShowEachResponse_(isShowEachResponse) {
#if 0
        ::printf("blockSize %zu nThreads %u isShowEachResponse %d\n",
                 blockSize_, nThreads_, isShowEachResponse_);
#endif
        assert(nThreads > 0);
        for (unsigned int i = 0; i < nThreads; i++) {

            bool isDirect = true;
            BlockDevice bd(name, mode, isDirect);
            ThreadLocalData threadLocal(std::move(bd), blockSize);
            threadLocal_.push_back(std::move(threadLocal));
        }
        assert(threadLocal_.size() == nThreads);
        maxBlockId_ = threadLocal_[0].getBlockDeviceSize();
    }
    ~IoThroughputBench() noexcept {}

    /**
     * @n Number of blocks to issue.
     * @startBlockId Start block id [block].
     */
    void execNtimes(size_t n, size_t startBlockId) {

        ThreadPoolWithId<size_t> threadPool(
            nThreads_, queueSize_,
            [&](size_t blockId, unsigned int id) {
                this->doWork(blockId, id);
            });

        size_t endBlockId = std::min(maxBlockId_, startBlockId + n);
        
        for (size_t i = startBlockId; i < endBlockId; i++) {
            threadPool.submit(i);
        }
        threadPool.flush(); threadPool.stop(); threadPool.join();
        threadPool.get(); //may throw an excpetion
                          //if errors have been occurred.
    }

    /**
     * @runPeriodInSec Run period [second].
     * @startBlockId Start block id [block].
     */
    void execNsecs(size_t runPeriodInSec, size_t startBlockId) {
        
        ThreadPoolWithId<size_t> threadPool(
            nThreads_, queueSize_,
            [&](size_t blockId, unsigned int id) {
                this->doWork(blockId, id);
            });
        
        std::atomic<bool> shouldStop(false);
        std::thread th([&] {
                size_t blockId = startBlockId;
                while (!shouldStop.load()) {
                    threadPool.submit(blockId);
                    blockId++;
                    if (blockId >= maxBlockId_) {
                        threadPool.flush();
                        threadPool.stop();
                        break;
                    }
                }
            });
        threadPool.waitFor(std::chrono::seconds(runPeriodInSec));
        shouldStop.store(true);
        th.join();
        threadPool.flush(); threadPool.stop(); threadPool.join();
        threadPool.get(); //may throw an exception.
                          //if errors have been occurred.
    }
    
    PerformanceStatistics getStat(unsigned int id) {

        return threadLocal_[id].getPerformanceStatistics();
    }

    PerformanceStatistics getMergedStat() {
        
        auto li = getStatsList();
        return mergeStats(li.begin(), li.end());
    }

    /**
     * Get the log queue of the thread with 'id'.
     */
    std::queue<IoLog>& getLogQueue(unsigned int id) {
        
        return threadLocal_[id].getLogQueue();
    }

private:
    /**
     * Execute an IO.
     *
     * @blockId block id [block]
     * @id Thread id (starting from 0).
     */
    void doWork(size_t blockId, unsigned int id) {

        bool isWrite = (mode_ == WRITE_MODE);

        auto& tLocal = threadLocal_[id];
        auto& bd = tLocal.getBlockDevice();
        char* buf = tLocal.getBuffer();
        auto& stat = tLocal.getPerformanceStatistics();
        
        IoLog log = execBlockIO(bd, id, isWrite, blockId, buf);

        if (isShowEachResponse_) { tLocal.getLogQueue().push(log); }
        stat.updateRt(log.response);
    }

    /**
     * @return IO log.
     */
    IoLog execBlockIO(BlockDevice& bd, unsigned int threadId, bool isWrite, size_t blockId, char* buf) {
        
        double begin, end;
        size_t oft = blockId * blockSize_;
        begin = getTime();
        
        if (isWrite) {
            bd.write(oft, blockSize_, buf);
        } else {
            bd.read(oft, blockSize_, buf);
        }
        end = getTime();

        return IoLog(threadId, isWrite, blockId, begin, end - begin);
    }

    /**
     * Print each statistics and all-merged statistics.
     */
    std::list<PerformanceStatistics> getStatsList() {

        std::list<PerformanceStatistics> ret;
        std::for_each(threadLocal_.begin(), threadLocal_.end(), [&](ThreadLocalData& tLocal) {
                ret.push_back(tLocal.getPerformanceStatistics());
            });
        return std::move(ret);
    }
};

/**
 * Use thread for parallel IO execution.
 */
void execThreadExperiment(const Options& opt)
{
    IoThroughputBench bench(
        opt.getArgs()[0], opt.getMode(), opt.getBlockSize(),
        opt.getNthreads(), opt.getQueueSize(), opt.isShowEachResponse());
    
    double begin, end;
    begin = getTime();
    try {
        if (opt.getPeriod() > 0) {
            bench.execNsecs(opt.getPeriod(), opt.getStartBlockId());
        } else {
            bench.execNtimes(opt.getCount(), opt.getStartBlockId());
        }
    } catch (const BlockDevice::EofError& e) {
        ::printf("EofError.\n");
    }
    end = getTime();

    /* print each IO log. */
    if (opt.isShowEachResponse()) {
        for (unsigned int id = 0; id < opt.getNthreads(); id++) {

            auto& logQ = bench.getLogQueue(id);
            while (!logQ.empty()) {
                logQ.front().print();
                logQ.pop();
            }
        }
    }

    /* Print statistics. */
    for (unsigned int id = 0; id < opt.getNthreads(); id++) {

        ::printf("threadId %u ", id);
        bench.getStat(id).print();
    }
    auto stat = bench.getMergedStat();
    ::printf("----------------\n"
             "all ");
    stat.print();
    printThroughput(opt.getBlockSize(), stat.getCount(), end - begin);
}


/**
 * Asynchronous IO throughptu benchmark.
 * This is single-thread.
 */
class AioThroughputBench
{
private:

    const std::string name_;
    const Mode mode_;
    const size_t blockSize_; /* [byte] */
    const unsigned int nThreads_;
    const unsigned int queueSize_;
    const bool isShowEachResponse_;

    std::queue<IoLog> logQ_;
    PerformanceStatistics stat_;
    BlockDevice bd_;
    Aio aio_;
    const size_t maxBlockId_;
    
public:
    /**
     * @param dev block device.
     * @param bs block size.
     * @param nBlocks disk size as number of blocks.
     * @param startBlockId 
     */
    AioThroughputBench(
        const std::string& name, const Mode mode, size_t blockSize,
        unsigned int nThreads, unsigned int queueSize, bool isShowEachResponse)
        : name_(name)
        , mode_(mode)
        , blockSize_(blockSize)
        , nThreads_(nThreads)
        , queueSize_(queueSize)
        , isShowEachResponse_(isShowEachResponse)
        , bd_(name, mode, true)
        , aio_(bd_.getFd(), queueSize)
        , maxBlockId_(bd_.getDeviceSize() / blockSize) {
#if 0
        ::printf("blockSize %zu nThreads %u isShowEachResponse %d\n",
                 blockSize_, nThreads_, isShowEachResponse_);
#endif
        assert(nThreads == 0);
        assert(queueSize > 0);
    }
    ~AioThroughputBench() noexcept {}

    /**
     * @n Number of blocks to issue.
     * @startBlockId Start block id [block].
     */
    void execNtimes(size_t n, size_t startBlockId) {

        BlockBuffer bb(queueSize_ * 2, blockSize_);

        size_t pending = 0;
        size_t blockId = startBlockId;
        size_t endBlockId = std::min(maxBlockId_, startBlockId + n);

        /* Fill the queue. */
        while (pending < queueSize_ && blockId < endBlockId) {
            prepareIo(blockId++, bb.next());
            pending++;
        }
        aio_.submit();
        /* Wait and fill. */
        while (blockId < endBlockId) {

            assert(pending == queueSize_);
            
            waitAnIo();
            pending--;

            prepareIo(blockId++, bb.next());
            pending++;
            aio_.submit();
        }
        /* Wait remaining. */
        while (pending > 0) {
            waitAnIo();
            pending--;
        }
    }

    /**
     * @runPeriodInSec Run period [second].
     * @startBlockId Start block id [block].
     */
    void execNsecs(size_t runPeriodInSec, size_t startBlockId) {
        
        BlockBuffer bb(queueSize_ * 2, blockSize_);

        size_t pending = 0;
        size_t blockId = startBlockId;

        double beginTime, endTime;
        beginTime = getTime();
        endTime = beginTime;
        
        /* Fill the queue. */
        while (pending < queueSize_ && blockId < maxBlockId_) {
            prepareIo(blockId++, bb.next());
            pending++;
        }
        aio_.submit();
        /* Wait and fill. */
        while (endTime - beginTime < static_cast<double>(runPeriodInSec)
               && blockId < maxBlockId_) {

            assert(pending == queueSize_);
            
            endTime = waitAnIo();
            pending--;

            prepareIo(blockId++, bb.next());
            pending++;
            aio_.submit();
        }
        /* Wait remaining. */
        while (pending > 0) {
            waitAnIo();
            pending--;
        }
    }

    /**
     * Get the performance statistics.
     */
    PerformanceStatistics& getStat() {

        return stat_;
    }

    /**
     * Get the log queue of the thread with 'id'.
     */
    std::queue<IoLog>& getLogQueue() {
        
        return logQ_;
    }

private:
    void prepareIo(size_t blockId, char *buf) {

        if (mode_ == WRITE_MODE) {
            aio_.prepareWrite(blockId * blockSize_, blockSize_, buf);
        } else {
            aio_.prepareRead(blockId * blockSize_, blockSize_, buf);
        }
    }

    double waitAnIo() {

        auto ptr = aio_.waitOne();
        auto log = toIoLog(ptr);
        stat_.updateRt(log.response);
        if (isShowEachResponse_) { 
            logQ_.push(log);
        }
        return ptr->endTime;
    }

    IoLog toIoLog(AioDataPtr ptr) {

        return IoLog(0, ptr->isWrite, ptr->oft / ptr->size,
                     ptr->beginTime, ptr->endTime - ptr->beginTime);
    }
};

/**
 * Use aio for parallel IO execution.
 */
void execAioExperiment(const Options& opt)
{
    AioThroughputBench bench(
        opt.getArgs()[0], opt.getMode(), opt.getBlockSize(),
        opt.getNthreads(), opt.getQueueSize(), opt.isShowEachResponse());
    
    double begin, end;
    begin = getTime();
    try {
        if (opt.getPeriod() > 0) {
            bench.execNsecs(opt.getPeriod(), opt.getStartBlockId());
        } else {
            bench.execNtimes(opt.getCount(), opt.getStartBlockId());
        }
    } catch (const Aio::EofError& e) {
        ::printf("EofError.\n");
    }
    end = getTime();

    /* print each IO log. */
    if (opt.isShowEachResponse()) {
        auto& logQ = bench.getLogQueue();
        while (!logQ.empty()) {
            logQ.front().print();
            logQ.pop();
        }
    }

    /* Statistics */
    auto& stat = bench.getStat();
    ::printf("all ");
    stat.print();
    printThroughput(opt.getBlockSize(), stat.getCount(), end - begin);
}

int main(int argc, char* argv[])
{
    ::srand(::time(0) + ::getpid());

    try {
        Options opt(argc, argv);

        if (opt.isShowVersion()) {
            opt.showVersion();
        } else if (opt.isShowHelp()) {
            opt.showHelp();
        } else {
            if (opt.getNthreads() == 0) {
                execAioExperiment(opt);
            } else {
                execThreadExperiment(opt);
            }
        }
    } catch (const std::runtime_error& e) {
        ::printf("error: %s\n", e.what());
    } catch (...) {
        ::printf("caught another error.\n");
    }
    
    return 0;
}

/* end of file. */
