/**
 * @file
 * @brief To know IO response time of file or device.
 * @author HOSHINO Takashi
 */
#define _FILE_OFFSET_BITS 64

#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <queue>
#include <utility>
#include <tuple>
#include <algorithm>
#include <future>
#include <mutex>
#include <exception>

#include <cstdio>
#include <cassert>
#include <cstring>
#include <cstdlib>
#include <cerrno>

#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "ioreth.hpp"
#include "util.hpp"

class IoResponseBench
{
private:
    static std::mutex mutex_;
    
    const int threadId_;
    BlockDevice& dev_;
    size_t blockSize_;
    size_t accessRange_;
    void* bufV_;
    char* buf_;
    std::queue<IoLog>& rtQ_;
    PerformanceStatistics& stat_;
    bool isShowEachResponse_;

public:
    /**
     * @param dev block device.
     * @param bs block size.
     * @param accessRange in blocks.
     */
    IoResponseBench(int threadId, BlockDevice& dev, size_t blockSize,
                    size_t accessRange, std::queue<IoLog>& rtQ,
                    PerformanceStatistics& stat,
                    bool isShowEachResponse)
        : threadId_(threadId)
        , dev_(dev)
        , blockSize_(blockSize)
        , accessRange_(calcAccessRange(accessRange, blockSize, dev))
        , bufV_(nullptr)
        , buf_(nullptr)
        , rtQ_(rtQ)
        , stat_(stat)
        , isShowEachResponse_(isShowEachResponse) {
#if 0
        ::printf("blockSize %zu accessRange %zu isShowEachResponse %d\n",
                 blockSize_, accessRange_, isShowEachResponse_);
#endif
        size_t alignSize = 512;
        while (alignSize < blockSize_) {
            alignSize *= 2;
        }
        if(::posix_memalign(&bufV_, alignSize, blockSize_) != 0) {
            throw std::runtime_error("posix_memalign failed");
        }
        buf_ = static_cast<char*>(bufV_);
        
        for (size_t i = 0; i < blockSize_; i ++) {
            buf_[i] = static_cast<char>(getRandomInt(256));
        }
    }
    ~IoResponseBench() {

        ::free(bufV_);
    }
    void execNtimes(size_t n) {

        for (size_t i = 0; i < n; i ++) {
            IoLog log = execBlockIO();
            if (isShowEachResponse_) { rtQ_.push(log); }
            stat_.updateRt(log.response);
        }
        putStat();
    }
    void execNsecs(size_t n) {

        double begin, end;
        begin = getTime(); end = begin;

        while (end - begin < static_cast<double>(n)) {

            IoLog log = execBlockIO();
            if (isShowEachResponse_) { rtQ_.push(log); }
            stat_.updateRt(log.response);
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
    IoLog execBlockIO() {
        
        double begin, end;
        size_t blockId = (size_t)getRandomInt(accessRange_);
        size_t oft = blockId * blockSize_;
        begin = getTime();
        bool isWrite = false;
        
        switch(dev_.getMode()) {
        case READ_MODE:  isWrite = false; break;
        case WRITE_MODE: isWrite = true; break;
        case MIX_MODE:   isWrite = (getRandomInt(2) == 0); break;
        }
        
        if (isWrite) {
            dev_.write(oft, blockSize_, buf_);
        } else {
            dev_.read(oft, blockSize_, buf_);
        }
        end = getTime();
        return IoLog(threadId_, isWrite, blockId, begin, end - begin);
    }

    void putStat() const {
        std::lock_guard<std::mutex> lk(mutex_);

        ::printf("id %d ", threadId_);
        stat_.print();
    }

    /**
     * Helper function for constructor.
     * Do not touch other members.
     */
    size_t calcAccessRange(size_t accessRange, size_t blockSize, BlockDevice& dev) {
    
        return (accessRange == 0) ? (dev.getDeviceSize() / blockSize) : accessRange;
    }
};

std::mutex IoResponseBench::mutex_;

class Options
{
private:
    std::string programName_;
    size_t accessRange_;
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
        : accessRange_(0)
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
                 "    -s size: access range in blocks.\n"
                 "    -b size: blocksize in bytes.\n"
                 "    -p secs: execute period in seconds.\n"
                 "    -c num:  number of IOs to execute.\n"
                 "             -p and -c is exclusive.\n"
                 "    -w:      write instead read.\n"
                 "    -m:      read/write mix instead read.\n"
                 "             -w and -m is exclusive.\n"
                 "    -t num:  number of threads in parallel.\n"
                 "    -q size: queue size per thread.\n"
                 "    -r:      show response of each IO.\n"
                 "    -v:      show version.\n"
                 "    -h:      show this help.\n"
                 , programName_.c_str()
            );
    }

    const std::vector<std::string>& getArgs() const { return args_; }
    size_t getAccessRange() const { return accessRange_; }
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
            int c = ::getopt(argc, argv, "s:b:p:c:t:q:wmrvh");

            if (c < 0) { break; }

            switch (c) {
            case 's': /* disk access range in blocks */
                accessRange_ = ::atol(optarg);
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
            case 'm': /* mix */
                mode_ = MIX_MODE;
                break;
            case 't': /* nthreads */
                nthreads_ = ::atol(optarg);
                break;
            case 'q':
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
            args_.push_back(argv[optind ++]);
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
        if (nthreads_ == 0) {
            throw std::runtime_error("number of threads (-t) must be 1 or more.");
        }
    }

};


void do_work(int threadId, const Options& opt, std::queue<IoLog>& rtQ, PerformanceStatistics& stat)
{
    const bool isDirect = true;

    BlockDevice bd(opt.getArgs()[0], opt.getMode(), isDirect);
    
    IoResponseBench bench(threadId, bd, opt.getBlockSize(), opt.getAccessRange(),
                          rtQ, stat, opt.isShowEachResponse());
    if (opt.getPeriod() > 0) {
        bench.execNsecs(opt.getPeriod());
    } else {
        bench.execNtimes(opt.getCount());
    }
}

void worker_start(std::vector<std::future<void> >& workers, int n, const Options& opt,
                  std::vector<std::queue<IoLog> >& rtQs, std::vector<PerformanceStatistics>& stats)
{

    rtQs.resize(n);
    stats.resize(n);
    for (int i = 0; i < n; i ++) {

        std::future<void> f = std::async(
            std::launch::async, do_work, i, std::ref(opt), std::ref(rtQs[i]), std::ref(stats[i]));
        workers.push_back(std::move(f));
    }
}

void worker_join(std::vector<std::future<void> >& workers)
{
    std::for_each(workers.begin(), workers.end(),
                  [](std::future<void>& f) { f.get(); });
}

void pop_and_show_rtQ(std::queue<IoLog>& rtQ)
{
    while (! rtQ.empty()) {
        IoLog& log = rtQ.front();
        log.print();
        rtQ.pop();
    }
}

void execExperiment(const Options& opt)
{
    const size_t nthreads = opt.getNthreads();
    assert(nthreads > 0);

    std::vector<std::queue<IoLog> > rtQs;
    std::vector<PerformanceStatistics> stats;
    
    std::vector<std::future<void> > workers;
    double begin, end;
    begin = getTime();
    worker_start(workers, nthreads, opt, rtQs, stats);
    worker_join(workers);
    end = getTime();

    assert(rtQs.size() == nthreads);
    std::for_each(rtQs.begin(), rtQs.end(), pop_and_show_rtQ);

    PerformanceStatistics stat = mergeStats(stats.begin(), stats.end());
    ::printf("---------------\n"
             "all ");
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
            execExperiment(opt);
        }
        
    } catch (const std::runtime_error& e) {

        ::printf("error: %s\n", e.what());
    } catch (...) {

        ::printf("caught another error.\n");
    }
    
    return 0;
}
