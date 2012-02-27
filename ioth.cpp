/**
 * @file
 * @brief To know IO throughput of file or device.
 * @author HOSHINO Takashi
 */
#define _FILE_OFFSET_BITS 64

#define IOTH_VERSION "1.0"

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

#include "util.hpp"
#include "thread_pool.hpp"



typedef std::shared_ptr<BlockDevice> BlockDevicePtr;

class IoThroughputBench
{
private:
    //static std::mutex mutex_;

    const std::string name_;
    const Mode mode_;
    const size_t blockSize_; /* [byte] */
    const unsigned int nThreads_;
    const unsigned taskQueueLength_;
    const bool isShowEachResponse_;
    
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

            if(::posix_memalign((void **)&buf_, blockSize, blockSize) != 0) {
                std::string e("posix_memalign failed");
                throw e;
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
        
        ~ThreadLocalData() throw() { free(buf_); }

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
                      unsigned int nThreads, unsigned taskQueueLength, bool isShowEachResponse)
        : name_(name)
        , mode_(mode)
        , blockSize_(blockSize)
        , nThreads_(nThreads)
        , taskQueueLength_(taskQueueLength)
        , isShowEachResponse_(isShowEachResponse) {
#if 0
        ::printf("blockSize %zu nThreads %u isShowEachResponse %d\n",
                 blockSize_, nThreads_, isShowEachResponse_);
#endif
        assert(nThreads > 0);
        for (unsigned int i = 0; i < nThreads; i ++) {

            bool isDirect = true;
            BlockDevice bd(name, mode, isDirect);
            ThreadLocalData threadLocal(std::move(bd), blockSize);
            threadLocal_.push_back(std::move(threadLocal));
        }
        assert(threadLocal_.size() == nThreads);
    }
    ~IoThroughputBench() throw() {}


    /**
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
     * @n Number of blocks to issue.
     * @startBlockId Start block id [block].
     */
    void execNtimes(size_t n, size_t startBlockId) {

        ThreadPoolWithId<size_t> threadPool(
            nThreads_, taskQueueLength_,
            [&](size_t blockId, unsigned int id) {
                this->doWork(blockId, id);
            });
        
        for (size_t i = startBlockId; i < startBlockId + n; i ++) {
            threadPool.submit(i);
        }
        threadPool.flush(); threadPool.stop(); threadPool.join();
        putAllStats();
    }

    /**
     * @runPeriodInSec Run period [second].
     * @startBlockId Start block id [block].
     */
    void execNsecs(size_t runPeriodInSec, size_t startBlockId) {

        ThreadPoolWithId<size_t> threadPool(
            nThreads_, taskQueueLength_,
            [&](size_t blockId, unsigned int id) {
                this->doWork(blockId, id);
            });
        
        volatile bool shouldStop = false;
        std::thread th([&] {
                size_t blockId = startBlockId;
                while (!shouldStop) {
                    threadPool.submit(blockId);
                    blockId ++;
                }
            });
        std::this_thread::sleep_for(std::chrono::seconds(runPeriodInSec));
        shouldStop = true;
        th.join();
        threadPool.flush(); threadPool.stop(); threadPool.join();
        putAllStats();
    }
    
private:
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

        return IoLog(threadId, isWrite, blockId, begin, end);
    }

    void putAllStats() {

        for (unsigned int i = 0; i < threadLocal_.size(); i ++) {
            ::printf("threadId %u ", i);
            threadLocal_[i].getPerformanceStatistics().put();
        }
        auto li = getStatsList();
        auto stat = mergeStats(li.begin(), li.end());
        
        ::printf("threadId all ");
        stat.put();
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


int main(int argc, char* argv[])
{


    return 0;
}


/*******************************************************************************
 * Old code.
 *******************************************************************************/

#if 0

class IoResponseBench
{
private:
    static std::mutex mutex_;
    
    const int threadId_;
    BlockDevice& dev_;
    size_t blockSize_;
    size_t nBlocks_;
    void* bufV_;
    char* buf_;
    std::queue<Res>& rtQ_;
    PerformanceStatistics& stat_;
    bool isShowEachResponse_;

public:
    /**
     * @param dev block device.
     * @param bs block size.
     * @param nBlocks disk size as number of blocks.
     */
    IoResponseBench(int threadId, BlockDevice& dev, size_t blockSize,
                    size_t nBlocks, std::queue<Res>& rtQ,
                    PerformanceStatistics& stat,
                    bool isShowEachResponse)
        : threadId_(threadId)
        , dev_(dev)
        , blockSize_(blockSize)
        , nBlocks_(nBlocks)
        , bufV_(nullptr)
        , buf_(nullptr)
        , rtQ_(rtQ)
        , stat_(stat)
        , isShowEachResponse_(isShowEachResponse) {
#if 0
        ::printf("blockSize %zu nBlocks %zu isShowEachResponse %d\n",
                 blockSize_, nBlocks_, isShowEachResponse_);
#endif
        if(::posix_memalign(&bufV_, blockSize_, blockSize_) != 0) {
            std::string e("posix_memalign failed");
            throw e;
        }
        buf_ = static_cast<char*>(bufV_);
    }
    ~IoResponseBench() {

        ::free(bufV_);
    }
    void execNtimes(size_t n) {

        Res res;
        for (size_t i = 0; i < n; i ++) {
            res = execBlockIO();
            if (isShowEachResponse_) { rtQ_.push(res); }
            stat_.updateRt(std::get<2>(res));
        }
        putStat();
    }
    void execNsecs(size_t n) {

        double begin, end;
        begin = getTime(); end = begin;

        Res res;
        while (end - begin < static_cast<double>(n)) {

            res = execBlockIO();
            if (isShowEachResponse_) { rtQ_.push(res); }
            stat_.updateRt(std::get<2>(res));
            end = getTime();
        }
        putStat();
    }
    
private:
    int getRandomInt(int max) {

        double maxF = static_cast<double>(max);
        return static_cast<int>(maxF * (::rand() / (RAND_MAX + 1.0)));
    }
    /**
     * @return response time.
     */
    std::tuple<int, bool, double> execBlockIO() {
        
        double begin, end;
        size_t oft = (size_t)getRandomInt(nBlocks_) * blockSize_;
        begin = getTime();
        bool isWrite = false;
        
        switch(dev_.getMode()) {
        case READ:  isWrite = false; break;
        case WRITE: isWrite = true; break;
        case MIX:   isWrite = (getRandomInt(2) == 0); break;
        }
        
        if (isWrite) {
            dev_.write(oft, blockSize_, buf_);
        } else {
            dev_.read(oft, blockSize_, buf_);
        }
        end = getTime();
        return std::tuple<int, bool, double>(threadId_, isWrite, end - begin);
    }

    void putStat() const {
        std::lock_guard<std::mutex> lk(mutex_);

        ::printf("id %d ", threadId_);
        stat_.put();
    }
};

std::mutex IoResponseBench::mutex_;

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

public:
    Options(int argc, char* argv[])
        : diskSize_(0)
        , startBlockId_(0)
        , args_()
        , mode_(READ)
        , isShowEachResponse_(false)
        , isShowVersion_(false)
        , isShowHelp_(false)
        , period_(0)
        , count_(0)
        , nthreads_(1) {

        parse(argc, argv);

        if (isShowVersion_ || isShowHelp_) {
            return;
        }
        if (args_.size() != 1 || diskSize_ == 0 || blockSize_ == 0) {
            throw std::string("specify disksize (-s), blocksize (-b), and device.");
        }
        if (period_ == 0 && count_ == 0) {
            throw std::string("specify period (-p) or count (-c).");
        }
    }

    void parse(int argc, char* argv[]) {

        programName_ = argv[0];
        
        while (1) {
            int c = ::getopt(argc, argv, "s:b:p:c:t:wrvh");

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
                mode_ = WRITE;
                break;
            case 't': /* nthreads */
                nthreads_ = ::atol(optarg);
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
            args_.push_back(argv[optind ++]);
        }
    }

    void showVersion() {

        ::printf("iores version %s\n", IORES_VERSION);
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
};


void do_work(int threadId, const Options& opt, std::queue<Res>& rtQ, PerformanceStatistics& stat)
{
    const size_t sizeInBytes = opt.getDiskSize() * opt.getBlockSize();
    const bool isDirect = true;

    BlockDevice bd(opt.getArgs()[0], sizeInBytes, opt.getMode(), isDirect);
    IoThroughputBench bench(threadId, bd, opt.getBlockSize(), opt.getStartBlockId(),
                          rtQ, stat, opt.isShowEachResponse());
    if (opt.getPeriod() > 0) {
        bench.execNsecs(opt.getPeriod());
    } else {
        bench.execNtimes(opt.getCount());
    }
}

void worker_start(std::vector<std::future<void> >& workers, int n, const Options& opt,
                  std::vector<std::queue<Res> >& rtQs, std::vector<PerformanceStatistics>& stats)
{

    rtQs.resize(n);
    stats.resize(n);
    for (int i = 0; i < n; i ++) {

        std::future<void> f = std::async(std::launch::async, do_work, i, std::ref(opt), std::ref(rtQs[i]), std::ref(stats[i]));
        workers.push_back(std::move(f));
    }
}

void worker_join(std::vector<std::future<void> >& workers)
{
    std::for_each(workers.begin(), workers.end(),
                  [](std::future<void>& f) { f.get(); });
}

void pop_and_show_rtQ(std::queue<Res>& rtQ)
{
    while (! rtQ.empty()) {
        Res res = rtQ.front();
        int threadId = std::get<0>(res);
        bool isWrite = std::get<1>(res);
        double rt = std::get<2>(res);
        ::printf("thread %d response %.06f %s\n",
                 threadId, rt, (isWrite ? "write" : "read"));
        rtQ.pop();
    }
}

void execExperiment(const Options& opt)
{
    const size_t nthreads = opt.getNthreads();
    assert(nthreads > 0);

    std::vector<std::queue<Res> > rtQs;
    std::vector<PerformanceStatistics> stats;
    
    std::vector<std::future<void> > workers;
    worker_start(workers, nthreads, opt, rtQs, stats);
    worker_join(workers);

    assert(rtQs.size() == nthreads);
    std::for_each(rtQs.begin(), rtQs.end(), pop_and_show_rtQ);

    PerformanceStatistics stat = mergeStats(stats);
    ::printf("---------------\n"
             "all %zu ", nthreads);
    stat.put();
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
            execExperiment(opt);
        }
        
    } catch (const std::string& e) {

        ::printf("error: %s\n", e.c_str());
    } catch (...) {

        ::printf("caught another error.\n");
    }
    
    return 0;
}
#endif
