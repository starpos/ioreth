/**
 * @file
 * @brief Utility header.
 * @author HOSHINO Takashi
 */
#ifndef UTIL_HPP
#define UTIL_HPP

#define _FILE_OFFSET_BITS 64

#include <vector>
#include <string>
#include <algorithm>

#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <linux/fs.h>

/* (threadId, isWrite, resonse time) */
typedef std::tuple<int, bool, double> Res;

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
    const std::string name_;
    const Mode mode_;
    int fd_;
    const size_t deviceSize_;

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
    explicit BlockDevice(BlockDevice&& bd)
        : name_(std::move(bd.name_))
        , mode_(bd.mode_)
        , fd_(bd.fd_)
        , deviceSize_(bd.deviceSize_) {

        bd.fd_ = -1;
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

    /**
     * Read data and fill a buffer.
     */
    void read(off_t oft, size_t size, char* buf) {

        if (deviceSize_ < oft + size) { throw std::string("range error."); }
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
    /**
     * Write data of a buffer.
     */
    void write(off_t oft, size_t size, char* buf) {

        if (deviceSize_ < oft + size) { throw std::string("range error."); }
        if (mode_ == READ_MODE) { throw std::string("write is not permitted."); }
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
    const Mode getMode() const { return mode_; }

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
            throw ss.str();
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
            throw ss.str();
        }
        if ((s.st_mode & S_IFMT) == S_IFBLK) {
            size_t size;
            if (::ioctl(fd_, BLKGETSIZE64, &size) < 0) {
                std::stringstream ss;
                ss << "ioctl failed: " << name_ << " " << ::strerror(errno) << ".";
                throw ss.str();
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

static inline
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

#endif /* UTIL_HPP */
