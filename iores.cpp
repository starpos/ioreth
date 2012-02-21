/**
 * @file
 * @brief To know IO response time of file or device.
 * @author HOSHINO Takashi
 */
#define _FILE_OFFSET_BITS 64

#define IORES_VERSION "1.0"

#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <queue>
#include <utility>
#include <tuple>
#include <algorithm>
#include <thread>

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

/* (threadId, isWrite, resonse time) */
typedef std::tuple<int, bool, double> Res;

/* Attriubte macro. */
#define __UNUSED __attribute__((unused))

static inline double getTime()
{
    struct timeval tv;
    double t;

    ::gettimeofday(&tv, NULL);
    
    t = static_cast<double>(tv.tv_sec) +
        static_cast<double>(tv.tv_usec) / 1000000.0;
    return t;
}

enum Mode
{
    READ, WRITE, MIX
};

class BlockDevice
{
private:
    const std::string name_;
    const size_t size_;
    const Mode mode_;
    const bool isDirect_;
    int fd_;

public:
    BlockDevice(const std::string& name, size_t size, const Mode mode, bool isDirect)
        : name_(name)
        , size_(size)
        , mode_(mode)
        , isDirect_(isDirect)
        , fd_(-1) {
#if 0
        ::printf("device %s size %zu mode %d isDirect %d\n",
                 name_.c_str(), size_, mode_, isDirect_);
#endif
        int flags = 0;
        switch (mode_) {
        case READ:  flags = O_RDONLY; break;
        case WRITE: flags = O_WRONLY; break;
        case MIX:   flags = O_RDWR;   break;
        }
        if (isDirect_) { flags |= O_DIRECT; }

        fd_ = ::open(name_.c_str(), flags);
        if (fd_ < 0) {
            std::stringstream ss;
            ss << "open failed: " << name_
               << " " << ::strerror(errno) << ".";
            throw ss.str();
        }
    }
    ~BlockDevice() {

        if (fd_ > 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }
    void read(off_t oft, size_t size, char* buf) {

        if (size_ < oft + size) { throw std::string("range error."); }
        ::lseek(fd_, oft, SEEK_SET);
        size_t s = 0;
        while (s < size) {
            ssize_t ret = ::read(fd_, &buf[s], size - s);
            if (ret < 0) {
                std::string e("read failed");
                ::perror(e.c_str()); throw e;
            }
            s += ret;
        }
    }
    void write(off_t oft, size_t size, char* buf) {

        if (size_ < oft + size) { throw std::string("range error."); }
        if (mode_ == READ) { throw std::string("write is not permitted."); }
        ::lseek(fd_, oft, SEEK_SET);
        size_t s = 0;
        while (s < size) {

            ssize_t ret = ::write(fd_, &buf[s], size - s);
            if (ret < 0) {
                std::string e("write failed");
                ::perror(e.c_str()); throw e;
            }
            s += ret;
        }
    }
    Mode getMode() const { return mode_; }
};

class PerformanceStatistics
{
private:
    double total_;
    double max_;
    double min_;
    size_t count_;

public:
    PerformanceStatistics()
        : total_(0), max_(-1.0), min_(-1.0), count_(0) {}
    PerformanceStatistics(double total, double max, double min, size_t count)
        : total_(total), max_(max), min_(min), count_(count) {}

    void updateRt(double rt) {

        if (max_ < 0 || min_ < 0) {
            max_ = rt; min_ = rt;
        } else if (max_ < rt) {
            max_ = rt;
        } else if (min_ > rt) {
            min_ = rt;
        }
        total_ += rt;
        count_ ++;
    }
    
    double getMax() const { return max_; }
    double getMin() const { return min_; }
    double getTotal() const { return total_; }
    size_t getCount() const { return count_; }

    double getAverage() const { return total_ / (double)count_; }

    void put() const {
        ::printf("total %.06f count %zu avg %.06f max %.06f min %.06f\n",
                 getTotal(), getCount(), getAverage(),
                 getMax(), getMin());
    }
};

PerformanceStatistics mergeStats(std::vector<PerformanceStatistics>& stats)
{
    double total = 0;
    double max = -1.0;
    double min = -1.0;
    size_t count = 0;

    std::for_each(stats.begin(), stats.end(), [&](PerformanceStatistics& stat) {

            total += stat.getTotal();
            if (max < 0 || max < stat.getMax()) { max = stat.getMax(); }
            if (min < 0 || min > stat.getMin()) { min = stat.getMin(); }
            count += stat.getCount();
        });

    return PerformanceStatistics(total, max, min, count);
}

class IoResponseBench
{
private:
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

        __UNUSED double begin, end;
        begin = getTime();
        Res res;
        
        for (size_t i = 0; i < n; i ++) {
            res = execBlockIO();
            if (isShowEachResponse_) { rtQ_.push(res); }
            stat_.updateRt(std::get<2>(res));
        }
        end = getTime();
        stat_.put();
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
        stat_.put();
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
        size_t oft = getRandomInt(nBlocks_) * blockSize_;
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
};

class Options
{
private:
    std::string programName_;
    size_t diskSize_;
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
        , blockSize_(0)
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
            int c = ::getopt(argc, argv, "s:b:p:c:t:wmrvh");

            if (c < 0) { break; }

            switch (c) {
            case 's': /* disk size in blocks */
                diskSize_ = ::atol(optarg);
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
            case 'm': /* mix */
                mode_ = MIX;
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
                 "    -s size: disksize in blocks.\n"
                 "    -b size: blocksize in bytes.\n"
                 "    -p secs: execute period in seconds.\n"
                 "    -c num:  number of IOs to execute.\n"
                 "             -p and -c is exclusive.\n"
                 "    -w:      write instead read.\n"
                 "    -m:      read/write mix instead read.\n"
                 "             -w and -m is exclusive.\n"
                 "    -t num:  number of threads in parallel.\n"
                 "    -r:      show response of each IO.\n"
                 "    -v:      show version.\n"
                 "    -h:      show this help.\n"
                 , programName_.c_str()
            );
    }

    const std::vector<std::string>& getArgs() const { return args_; }
    size_t getDiskSize() const { return diskSize_; }
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
    IoResponseBench bench(threadId, bd, opt.getBlockSize(), opt.getDiskSize(),
                          rtQ, stat, opt.isShowEachResponse());
    if (opt.getPeriod() > 0) {
        bench.execNsecs(opt.getPeriod());
    } else {
        bench.execNtimes(opt.getCount());
    }
}

void worker_start(std::vector<std::thread>& workers, int n, const Options& opt,
                  std::vector<std::queue<Res> >& rtQs, std::vector<PerformanceStatistics>& stats)
{

    rtQs.resize(n);
    stats.resize(n);
    for (int i = 0; i < n; i ++) {

        auto th = std::thread(do_work, i, std::ref(opt), std::ref(rtQs[i]), std::ref(stats[i]));
        workers.push_back(std::move(th));
    }
}

void worker_join(std::vector<std::thread>& workers)
{
    std::for_each(workers.begin(), workers.end(),
                  [](std::thread& th) { th.join(); });
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
    
    std::vector<std::thread> workers;
    worker_start(workers, nthreads, opt, rtQs, stats);
    worker_join(workers);

    assert(rtQs.size() == nthreads);
    std::for_each(rtQs.begin(), rtQs.end(), pop_and_show_rtQ);

    PerformanceStatistics stat = mergeStats(stats);
    ::printf("---------------\n");
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
