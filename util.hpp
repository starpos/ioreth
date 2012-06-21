/**
 * @file
 * @brief Utility header.
 * @author HOSHINO Takashi
 */
#ifndef UTIL_HPP
#define UTIL_HPP

#define _FILE_OFFSET_BITS 64

#include <vector>
#include <queue>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <exception>
#include <cerrno>
#include <cstdio>

#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <libaio.h>

/**
 * Each IO log.
 */
struct IoLog
{
    const unsigned int threadId;
    const bool isWrite;
    const size_t blockId;
    const double startTime; /* unix time [second] */
    const double response; /* [second] */

    IoLog(unsigned int threadId_, bool isWrite_, size_t blockId_,
          double startTime_, double response_)
        : threadId(threadId_)
        , isWrite(isWrite_)
        , blockId(blockId_)
        , startTime(startTime_)
        , response(response_) {}

    void print() {
        ::printf("threadId %d isWrite %d blockId %10zu startTime %.06f response %.06f\n",
                 threadId, isWrite, blockId, startTime, response);
    }
};

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
    READ_MODE, WRITE_MODE, MIX_MODE
};

class BlockDevice
{
private:
    std::string name_;
    Mode mode_;
    int fd_;
    size_t deviceSize_;

public:
    BlockDevice(const std::string& name, const Mode mode, bool isDirect)
        : name_(name)
        , mode_(mode)
        , fd_(openDevice(name, mode, isDirect))
        , deviceSize_(getDeviceSizeFirst()) {
#if 0
        ::printf("device %s size %zu mode %d isDirect %d\n",
                 name_.c_str(), size_, mode_, isDirect_);
#endif
    }
    explicit BlockDevice(BlockDevice&& rhs)
        : name_(std::move(rhs.name_))
        , mode_(rhs.mode_)
        , fd_(rhs.fd_)
        , deviceSize_(rhs.deviceSize_) {

        rhs.fd_ = -1;
    }
    BlockDevice& operator=(BlockDevice&& rhs) {

        name_ = std::move(rhs.name_);
        mode_ = rhs.mode_;
        fd_ = rhs.fd_; rhs.fd_ = -1;
        deviceSize_= rhs.deviceSize_;
        return *this;
    }
    
    ~BlockDevice() {

        if (fd_ > 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    /**
     * Get device size [byte].
     */
    size_t getDeviceSize() const {

        return deviceSize_;
    }

    class EofError : public std::exception {};
    
    /**
     * Read data and fill a buffer.
     */
    void read(off_t oft, size_t size, char* buf) {

        if (deviceSize_ < oft + size) { throw EofError(); }
        ::lseek(fd_, oft, SEEK_SET);
        size_t s = 0;
        while (s < size) {
            ssize_t ret = ::read(fd_, &buf[s], size - s);
            if (ret < 0) {
                std::string e("read failed: ");
                e += strerror(errno);
                throw std::runtime_error(e);
            }
            s += ret;
        }
    }
    /**
     * Write data of a buffer.
     */
    void write(off_t oft, size_t size, char* buf) {

        if (deviceSize_ < oft + size) { throw EofError(); }
        if (mode_ == READ_MODE) { throw std::runtime_error("write is not permitted."); }
        ::lseek(fd_, oft, SEEK_SET);
        size_t s = 0;
        while (s < size) {
            ssize_t ret = ::write(fd_, &buf[s], size - s);
            if (ret < 0) {
                std::string e("write failed: ");
                e += ::strerror(errno);
                throw std::runtime_error(e);
            }
            s += ret;
        }
    }
    const Mode getMode() const { return mode_; }
    int getFd() const { return fd_; }

private:

    /**
     * Helper function for constructor.
     */
    int openDevice(const std::string& name, const Mode mode, bool isDirect) {

        int fd;
        int flags = 0;
        switch (mode) {
        case READ_MODE:  flags = O_RDONLY; break;
        case WRITE_MODE: flags = O_WRONLY; break;
        case MIX_MODE:   flags = O_RDWR;   break;
        }
        if (isDirect) { flags |= O_DIRECT; }

        fd = ::open(name_.c_str(), flags);
        if (fd < 0) {
            std::stringstream ss;
            ss << "open failed: " << name_
               << " " << ::strerror(errno) << ".";
            throw std::runtime_error(ss.str());
        }
        return fd;
    }
    
    /**
     * Helper function for constructor.
     * Get device size in bytes.
     */
    size_t getDeviceSizeFirst() const {

        size_t ret;
        struct stat s;
        if (::fstat(fd_, &s) < 0) {
            std::stringstream ss;
            ss << "fstat failed: " << name_ << " " << ::strerror(errno) << ".";
            throw std::runtime_error(ss.str());
        }
        if ((s.st_mode & S_IFMT) == S_IFBLK) {
            size_t size;
            if (::ioctl(fd_, BLKGETSIZE64, &size) < 0) {
                std::stringstream ss;
                ss << "ioctl failed: " << name_ << " " << ::strerror(errno) << ".";
                throw std::runtime_error(ss.str());
            }
            ret = size;
        } else {
            ret = s.st_size;
        }
#if 0        
        std::cout << "devicesize: " << ret << std::endl; //debug
#endif
        return ret;
    }
};

/**
 * An aio data.
 */
struct AioData
{
    struct iocb iocb;
    off_t oft;
    size_t size;
    char *buf;
    double beginTime;
    double endTime;
};

/**
 * Pointer to AioData.
 */
typedef std::shared_ptr<AioData> AioDataPtr;

/**
 * Asynchronous IO wrapper.
 */
class Aio
{
private:
    int fd_;
    size_t queueSize_;
    io_context_t ctx_;
    std::unordered_map<void *, AioDataPtr> aioMap_;
    std::queue<AioDataPtr> aioQueue_;

public:
    /**
     * @fd Opened file descripter.
     * @queueSize queue size for aio.
     */
    Aio(int fd, size_t queueSize)
        : fd_(fd)
        , queueSize_(queueSize) {

        assert(fd_ > 0);
        ::io_queue_init(queueSize_, &ctx_);
    }

    ~Aio() noexcept {

        ::io_queue_release(ctx_);
    }

    class EofError : public std::exception {};

    /**
     * Prepare a read IO.
     */
    AioDataPtr prepareRead(off_t oft, size_t size, char* buf) noexcept {

        aioQueue_.push(AioDataPtr(new AioData));
        auto& ptr = aioQueue_.back();
        ptr->oft = oft;
        ptr->size = size;
        ptr->buf = buf;
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ::io_prep_pread(&ptr->iocb, fd_, buf, size, oft);
        return ptr;
    }

    /**
     * Prepare a write IO.
     */
    AioDataPtr prepareWrite(off_t oft, size_t size, char* buf) noexcept {

        aioQueue_.push(AioDataPtr(new AioData));
        auto& ptr = aioQueue_.back();
        ptr->oft = oft;
        ptr->size = size;
        ptr->buf = buf;
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ::io_prep_pwrite(&ptr->iocb, fd_, buf, size, oft);
        return ptr;
    }

    /**
     * Submit all prepared IO(s).
     */
    void submit() {

        size_t nr = aioQueue_.size();
        if (nr == 0) {
            return;
        }
        std::vector<struct iocb *> iocbs(nr);
        double beginTime = getTime();
        for (size_t i = 0; i < nr; i++) {
            auto& ptr = aioQueue_.front();
            aioMap_.insert(std::make_pair(&ptr->iocb, ptr));
            aioQueue_.pop();
            iocbs[i] = &ptr->iocb;
            ptr->beginTime = beginTime;
        }
        int err = ::io_submit(ctx_, nr, &iocbs[0]);
        if (err != static_cast<int>(nr)) {
            /* ::printf("submit error %d.\n", err); */
            throw EofError();
        }
    }

    /**
     * Wait several IO(s) completed.
     *
     * @nr number of waiting IO(s).
     * @events event array for temporary use.
     * @aioQueue AioDataPtr of completed IO will be pushed into it.
     */
    void wait(size_t nr,
              std::vector<struct io_event>& events,
              std::queue<AioDataPtr>& aioQueue) {

        assert(nr = events.size());
        
        size_t done = 0;
        bool isError = false;
        while (done < nr) {
            int tmpNr = ::io_getevents(ctx_, 1, nr - done, &events[done], NULL);
            if (tmpNr < 1) {
                throw std::runtime_error("io_getevents failed.");
            }
            double endTime = getTime();
            for (size_t i = done; i < done + tmpNr; i++) {
                auto ptr = aioMap_[events[i].obj];
                aioMap_.erase(&ptr->iocb);
                if (events[i].res != ptr->iocb.u.c.nbytes) {
                    isError = true;
                }
                ptr->endTime = endTime;
                aioQueue.push(ptr);
            }
            done += tmpNr;
        }
        if (isError) {
            // ::printf("wait error.\n");
            throw EofError();
        }
    }

    /**
     * Wait just one IO completed.
     *
     * @return aio data pointer.
     */
    AioDataPtr waitOne() {

        struct io_event event;
        int err = ::io_getevents(ctx_, 1, 1, &event, NULL);
        double endTime = getTime();
        if (err != 1) {
            throw std::runtime_error("io_getevents failed.");
        }
        auto ptr = aioMap_[event.obj];
        aioMap_.erase(&ptr->iocb);
        if (event.res != ptr->iocb.u.c.nbytes) {
            // ::printf("waitOne error %lu\n", event.res);
            throw EofError();
        }
        ptr->endTime = endTime;
        return ptr;
    }
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

    void print() const {
        ::printf("total %.06f count %zu avg %.06f max %.06f min %.06f\n",
                 getTotal(), getCount(), getAverage(),
                 getMax(), getMin());
    }
};

template<typename T> //T is iterator type of PerformanceStatistics.
static inline PerformanceStatistics mergeStats(const T begin, const T end)
{
    double total = 0;
    double max = -1.0;
    double min = -1.0;
    size_t count = 0;

    std::for_each(begin, end, [&](PerformanceStatistics& stat) {

            total += stat.getTotal();
            if (max < 0 || max < stat.getMax()) { max = stat.getMax(); }
            if (min < 0 || min > stat.getMin()) { min = stat.getMin(); }
            count += stat.getCount();
        });
    
    return PerformanceStatistics(total, max, min, count);
}

/**
 * Convert throughput data to string.
 */
static inline
std::string getDataThroughputString(double throughput)
{
    const double GIGA = static_cast<double>(1000ULL * 1000ULL * 1000ULL);
    const double MEGA = static_cast<double>(1000ULL * 1000ULL);
    const double KILO = static_cast<double>(1000ULL);
    
    std::stringstream ss;
    if (throughput > GIGA) {
        throughput /= GIGA;
        ss << throughput << " GB/sec";
    } else if (throughput > MEGA) {
        throughput /= MEGA;
        ss << throughput << " MB/sec";
    } else if (throughput > KILO) {
        throughput /= KILO;
        ss << throughput << " KB/sec";
    } else {
        ss << throughput << " B/sec";
    }
    
    return ss.str();
}


/**
 * Print throughput data.
 * @blockSize block size [bytes].
 * @nio Number of IO executed.
 * @periodInSec Elapsed time [second].
 */
static inline
void printThroughput(size_t blockSize, size_t nio, double periodInSec)
{
    double throughput = static_cast<double>(blockSize * nio) / periodInSec;
    double iops = static_cast<double>(nio) / periodInSec;
    ::printf("Throughput: %.3f B/s %s %.3f iops.\n",
             throughput, getDataThroughputString(throughput).c_str(), iops);
}

#endif /* UTIL_HPP */
