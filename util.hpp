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
