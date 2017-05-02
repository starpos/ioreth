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
#include <limits>

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
#include "rand.hpp"
#include "unit_int.hpp"
#include "easy_signal.hpp"


class Options
{
private:
    std::string programName_;
    size_t accessRange_;
    size_t blockSize_;
    std::vector<std::string> args_;
    Mode mode_;
    bool dontUseOdirect_;
    bool isShowEachResponse_;
    bool isShowVersion_;
    bool isShowHelp_;

    size_t period_;
    size_t count_;
    size_t nthreads_;
    size_t queueSize_;
    size_t flushInterval_;
    size_t ignorePeriod_;
    size_t readPct_;

public:
    Options(int argc, char* argv[])
        : accessRange_(0)
        , blockSize_(0)
        , args_()
        , mode_(READ_MODE)
        , dontUseOdirect_(false)
        , isShowEachResponse_(false)
        , isShowVersion_(false)
        , isShowHelp_(false)
        , period_(0)
        , count_(0)
        , nthreads_(1)
        , queueSize_(1)
        , flushInterval_(0)
        , ignorePeriod_(0)
        , readPct_(0) {

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
                 "    -m pct:  read/write mix instead read. pct means read percentage from 1 to 99.\n"
                 "    -d:      discard instead read.\n"
                 "             -w, -m, and -d is exclusive.\n"
                 "    -t num:  number of threads in parallel.\n"
                 "             if 0, use aio instead thread.\n"
                 "    -q size: queue size per thread.\n"
                 "             this is meaningfull with -t 0.\n"
                 "    -f nIO:  flush interval [IO]. default: 0.\n"
                 "             0 means flush request will never occur.\n"
                 "    -i secs: start to measure performance after several seconds.\n"
                 "    -n:      do not use O_DIRECT.\n"
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
    bool dontUseOdirect() const { return dontUseOdirect_; }
    bool isShowEachResponse() const { return isShowEachResponse_; }
    bool isShowVersion() const { return isShowVersion_; }
    bool isShowHelp() const { return isShowHelp_; }
    size_t getPeriod() const { return period_; }
    size_t getCount() const { return count_; }
    size_t getNthreads() const { return nthreads_; }
    size_t getQueueSize() const { return queueSize_; }
    size_t getFlushInterval() const { return flushInterval_; }
    size_t getIgnorePeriod() const { return ignorePeriod_; }
    size_t getReadPct() const { return readPct_; }

private:
    void parse(int argc, char* argv[]) {

        programName_ = argv[0];

        while (1) {
            int c = ::getopt(argc, argv, "s:b:p:c:t:q:f:i:wmdrnvh");

            if (c < 0) { break; }

            switch (c) {
            case 's': /* disk access range in blocks */
                accessRange_ = fromUnitIntString(optarg);
                break;
            case 'b': /* blocksize */
                blockSize_ = fromUnitIntString(optarg);
                break;
            case 'p': /* period */
                period_ = ::atol(optarg);
                break;
            case 'c': /* count */
                count_ = fromUnitIntString(optarg);
                break;
            case 'w': /* write */
                mode_ = WRITE_MODE;
                break;
            case 'm': /* mix */
                mode_ = MIX_MODE;
                readPct_ = ::atol(optarg);
                break;
            case 'd': /* discard */
                mode_ = DISCARD_MODE;
                break;
            case 't': /* nthreads */
                nthreads_ = ::atol(optarg);
                break;
            case 'q': /* queue size */
                queueSize_ = ::atol(optarg);
                break;
            case 'f': /* flush interval */
                flushInterval_ = ::atol(optarg);
                break;
            case 'r': /* show each response */
                isShowEachResponse_ = true;
                break;
            case 'i': /* ignore period */
                ignorePeriod_ = ::atol(optarg);
                break;
            case 'n': /* do not use O_DIRECT */
                dontUseOdirect_ = true;
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
        if (nthreads_ == 0 && queueSize_ == 0) {
            throw std::runtime_error("queue size (-q) must be 1 or more when -t 0.");
        }
        if (readPct_ > 100) {
            throw std::runtime_error("read percentage must be between 0 and 100.");
        }
    }
};

volatile bool g_quit_ = false;


void quitHandler(int)
{
    g_quit_ = true;
}


/**
 * Single-threaded io response benchmark.
 */
class IoResponseBench
{
private:
    const int threadId_;
    BlockDevice& dev_;
    const size_t blockSize_;
    const size_t accessRange_;
    void* bufV_;
    char* buf_;
    std::queue<IoLog>& rtQ_;
    PerformanceStatistics& stat_;
    const bool isShowEachResponse_;
    XorShift128 rand_;
    const size_t flushInterval_;
    const size_t ignorePeriod_;
    const size_t readPct_;

    std::mutex& mutex_; //shared among threads.

public:
    /**
     * @param dev block device.
     * @param bs block size.
     * @param accessRange in blocks.
     */
    IoResponseBench(int threadId, BlockDevice& dev, size_t blockSize,
                    size_t accessRange, std::queue<IoLog>& rtQ,
                    PerformanceStatistics& stat,
                    bool isShowEachResponse,
                    size_t flushInterval, size_t ignorePeriod, size_t readPct,
                    std::mutex& mutex)
        : threadId_(threadId)
        , dev_(dev)
        , blockSize_(blockSize)
        , accessRange_(calcAccessRange(accessRange, blockSize, dev))
        , bufV_(nullptr)
        , buf_(nullptr)
        , rtQ_(rtQ)
        , stat_(stat)
        , isShowEachResponse_(isShowEachResponse)
        , rand_(getSeed())
        , flushInterval_(flushInterval)
        , ignorePeriod_(ignorePeriod)
        , readPct_(readPct)
        , mutex_(mutex) {
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

        for (size_t i = 0; i < blockSize_; i++) {
            buf_[i] = static_cast<char>(rand_.get(256));
        }
    }
    ~IoResponseBench() {
        ::free(bufV_);
    }
    void execNtimes(size_t n) {
        const double bgn = getTime();
        double end = bgn;

        for (size_t i = 0; i < n; i++) {
            if (g_quit_) break;
            bool isFlush = flushInterval_ > 0 &&
                i % flushInterval_ == flushInterval_ - 1;
            IoLog log(isFlush ? execFlushIO() : execBlockIO());
            end = log.startTime + log.response;
            if (end - bgn > static_cast<double>(ignorePeriod_)) {
                if (isShowEachResponse_) { rtQ_.push(log); }
                stat_.updateRt(log.response);
            }
        }
        putStat();
    }
    void execNsecs(size_t n) {
        const double bgn = getTime();
        double end = bgn;
        size_t i = 0;

        while (end - bgn < static_cast<double>(n)) {
            if (g_quit_) break;
            bool isFlush = flushInterval_ > 0 &&
                i % flushInterval_ == flushInterval_ - 1;
            IoLog log(isFlush ? execFlushIO() : execBlockIO());
            end = log.startTime + log.response;
            if (end - bgn > static_cast<double>(ignorePeriod_)) {
                if (isShowEachResponse_) { rtQ_.push(log); }
                stat_.updateRt(log.response);
            }
            i++;
        }
        putStat();
    }

private:
    /**
     * @return response time.
     */
    IoLog execBlockIO() {
        size_t blockId = rand_.get(accessRange_);
        size_t oft = blockId * blockSize_;

        bool isWrite = false;
        bool isDiscard = false;
        IoType type;
        switch(dev_.getMode()) {
        case READ_MODE:
            isWrite = false;
            type = IOTYPE_READ;
            break;
        case WRITE_MODE:
            isWrite = true;
            type = IOTYPE_WRITE;
            break;
        case MIX_MODE:
            isWrite = (rand_.get(100) >= readPct_);
            type = isWrite ? IOTYPE_WRITE : IOTYPE_READ;
            break;
        case DISCARD_MODE:
            isDiscard = true;
            type = IOTYPE_DISCARD;
        }

        double bgn = getTime();
        if (isDiscard) {
            dev_.discard(oft, blockSize_);
        } else if (isWrite) {
            dev_.write(oft, blockSize_, buf_);
        } else {
            dev_.read(oft, blockSize_, buf_);
        }
        double end = getTime();

        return IoLog(threadId_, type, blockId, bgn, end - bgn);
    }

    /**
     * @return response time.
     */
    IoLog execFlushIO() {
        double bgn = getTime();
        dev_.flush();
        double end = getTime();
        return IoLog(threadId_, IOTYPE_FLUSH, 0, bgn, end - bgn);
    }

    void putStat() const {
        std::lock_guard<std::mutex> lk(mutex_);

        ::printf("id %d ", threadId_);
        stat_.print();
    }

    uint32_t getSeed() const {
        Rand<uint32_t, std::uniform_int_distribution<uint32_t> >
            rand(0, std::numeric_limits<uint32_t>::max());
        return rand.get();
    }
};

void do_work(int threadId, const Options& opt,
             std::queue<IoLog>& rtQ, PerformanceStatistics& stat,
             std::mutex& mutex)
{
    const bool isDirect = !opt.dontUseOdirect();;

    BlockDevice bd(opt.getArgs()[0], opt.getMode(), isDirect);

    IoResponseBench bench(threadId, bd, opt.getBlockSize(), opt.getAccessRange(),
                          rtQ, stat, opt.isShowEachResponse(),
                          opt.getFlushInterval(), opt.getIgnorePeriod(), opt.getReadPct(), mutex);
    if (opt.getPeriod() > 0) {
        bench.execNsecs(opt.getPeriod());
    } else {
        bench.execNtimes(opt.getCount());
    }
}

void worker_start(std::vector<std::future<void> >& workers, int n, const Options& opt,
                  std::vector<std::queue<IoLog> >& rtQs,
                  std::vector<PerformanceStatistics>& stats,
                  std::mutex& mutex)
{
    rtQs.resize(n);
    stats.resize(n);
    for (int i = 0; i < n; i++) {

        std::future<void> f = std::async(
            std::launch::async, do_work, i, std::ref(opt), std::ref(rtQs[i]),
            std::ref(stats[i]), std::ref(mutex));
        workers.push_back(std::move(f));
    }
}

void worker_join(std::vector<std::future<void> >& workers)
{
    std::for_each(workers.begin(), workers.end(),
                  [](std::future<void>& f) { f.get(); });
}

void pop_and_show_logQ(std::queue<IoLog>& logQ)
{
    while (! logQ.empty()) {
        IoLog& log = logQ.front();
        log.print();
        logQ.pop();
    }
}

void execThreadExperiment(const Options& opt)
{
    const size_t nthreads = opt.getNthreads();
    assert(nthreads > 0);

    std::vector<std::queue<IoLog> > logQs;
    std::vector<PerformanceStatistics> stats;

    std::vector<std::future<void> > workers;
    std::mutex mutex;

    const double bgn = getTime();
    worker_start(workers, nthreads, opt, logQs, stats, mutex);
    worker_join(workers);
    const double end = getTime();

    assert(logQs.size() == nthreads);
    std::for_each(logQs.begin(), logQs.end(), pop_and_show_logQ);

    PerformanceStatistics stat = mergeStats(stats.begin(), stats.end());
    ::printf("---------------\n"
             "all ");
    stat.print();
    const double period =
        end - bgn - static_cast<double>(opt.getIgnorePeriod());
    if (period > 0) {
        printThroughput(opt.getBlockSize(), stat.getCount(), period);
    } else {
        printZeroThroughput();
    }
}

/**
 * Io response bench with aio.
 */
class AioResponseBench
{
private:
    const size_t blockSize_;
    const size_t queueSize_;
    const size_t accessRange_;
    const bool isShowEachResponse_;
    const size_t flushInterval_;
    const size_t ignorePeriod_;
    const Mode mode_;

    BlockBuffer bb_;
    Rand<size_t, std::uniform_int_distribution<size_t> > rand_;
    std::queue<IoLog> logQ_;
    PerformanceStatistics stat_;
    Aio aio_;
    double bgnTime_;

public:
    AioResponseBench(
        const BlockDevice& dev, size_t blockSize, size_t queueSize,
        size_t accessRange, bool isShowEachResponse,
        size_t flushInterval, size_t ignorePeriod)
        : blockSize_(blockSize)
        , queueSize_(queueSize)
        , accessRange_(calcAccessRange(accessRange, blockSize, dev))
        , isShowEachResponse_(isShowEachResponse)
        , flushInterval_(flushInterval)
        , ignorePeriod_(ignorePeriod)
        , mode_(dev.getMode())
        , bb_(queueSize * 2, blockSize)
        , rand_(0, std::numeric_limits<size_t>::max())
        , logQ_()
        , stat_()
        , aio_(dev.getFd(), queueSize)
        , bgnTime_(0) {

        assert(blockSize_ % 512 == 0);
        assert(queueSize_ > 0);
        assert(accessRange_ > 0);
    }

    void execNtimes(size_t nTimes) {
        bgnTime_ = getTime();
        size_t pending = 0;
        size_t c = 0;

        // Fill the queue.
        while (pending < queueSize_ && c < nTimes) {
            prepareIo(bb_.next());
            pending++;
            c++;
        }
        aio_.submit();
        // Wait and fill.
        while (c < nTimes) {
            assert(pending == queueSize_);

            waitAnIo();
            pending--;

            bool isFlush = flushInterval_ > 0 &&
                c % flushInterval_ == flushInterval_ - 1;
            if (isFlush) {
                prepareFlush();
            } else {
                prepareIo(bb_.next());
            }
            pending++;
            c++;
            aio_.submit();
        }
        // Wait remaining.
        while (pending > 0) {
            waitAnIo();
            pending--;
        }
    }

    void execNsecs(size_t nSecs) {
        bgnTime_ = getTime();
        double end = bgnTime_;
        size_t c = 0;
        size_t pending = 0;

        // Fill the queue.
        while (pending < queueSize_) {
            prepareIo(bb_.next());
            pending++;
            c++;
        }
        aio_.submit();
        // Wait and fill.
        while (end - bgnTime_ < static_cast<double>(nSecs)) {
            assert(pending == queueSize_);

            end = waitAnIo();
            pending--;

            bool isFlush = flushInterval_ > 0 &&
                c % flushInterval_ == flushInterval_ - 1;
            if (isFlush) {
                prepareFlush();
            } else {
                prepareIo(bb_.next());
            }

            pending++;
            c++;
            aio_.submit();
        }
        // Wait pending.
        while (pending > 0) {
            waitAnIo();
            pending--;
        }
    }

    PerformanceStatistics& getStat() { return stat_; }
    std::queue<IoLog>& getIoLogQueue() { return logQ_; }

private:
    bool decideIsWrite() {
        bool isWrite = false;

        switch(mode_) {
        case READ_MODE:
            isWrite = false;
            break;
        case WRITE_MODE:
            isWrite = true;
            break;
        case MIX_MODE:
            isWrite = rand_.get(2) == 0;
            break;
        default:
            assert(false);
        }
        return isWrite;
    }

    void prepareIo(char *buf) {
        size_t blockId = rand_.get(accessRange_);

        if (decideIsWrite()) {
            aio_.prepareWrite(blockId * blockSize_, blockSize_, buf);
        } else {
            aio_.prepareRead(blockId * blockSize_, blockSize_, buf);
        }
    }

    double waitAnIo() {
        auto* ptr = aio_.waitOne();
        auto log = toIoLog(ptr);
        if (ptr->endTime  - bgnTime_ > static_cast<double>(ignorePeriod_)) {
            stat_.updateRt(log.response);
            if (isShowEachResponse_) {
                logQ_.push(log);
            }
        }
        return ptr->endTime;
    }

    void prepareFlush() {
        aio_.prepareFlush();
    }

    IoLog toIoLog(AioData *ptr) {
        return IoLog(0, ptr->type, ptr->oft / ptr->size,
                     ptr->beginTime, ptr->endTime - ptr->beginTime);
    }
};

void execAioExperiment(const Options& opt)
{
    assert(opt.getNthreads() == 0);
    const size_t queueSize = opt.getQueueSize();
    assert(queueSize > 0);

    const bool isDirect = true;
    BlockDevice bd(opt.getArgs()[0], opt.getMode(), isDirect);

    AioResponseBench bench(bd, opt.getBlockSize(), opt.getQueueSize(),
                           opt.getAccessRange(),
                           opt.isShowEachResponse(),
                           opt.getFlushInterval(),
                           opt.getIgnorePeriod());

    const double bgn = getTime();
    if (opt.getPeriod() > 0) {
        bench.execNsecs(opt.getPeriod());
    } else {
        bench.execNtimes(opt.getCount());
    }
    const double end = getTime();

    pop_and_show_logQ(bench.getIoLogQueue());
    auto& stat = bench.getStat();
    ::printf("all ");
    stat.print();
    const double period =
        end - bgn - static_cast<double>(opt.getIgnorePeriod());
    if (period > 0) {
        printThroughput(opt.getBlockSize(), stat.getCount(), period);
    } else {
        printZeroThroughput();
    }
}

int main(int argc, char* argv[]) try
{
    if (!cybozu::signal::setSignalHandler(quitHandler, {SIGINT, SIGQUIT, SIGABRT, SIGTERM}, false)) {
        ::printf("could not set signal handler.");
        ::exit(1);
    }

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

    return 0;
} catch (const std::runtime_error& e) {
    ::fprintf(::stderr, "error: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error.\n");
}
