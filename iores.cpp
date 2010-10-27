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

#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

static inline double getTime()
{
    struct timeval tv;
    double t;

    ::gettimeofday(&tv, NULL);
    
    t = static_cast<double>(tv.tv_sec) +
        static_cast<double>(tv.tv_usec) / 1000000.0;
    return t;
}

class BlockDevice
{
private:
    std::string name_;
    const size_t size_;
    const bool isWrite_;
    const bool isDirect_;
    int fd_;

public:
    BlockDevice(const std::string& name, size_t size, bool isWrite, bool isDirect)
        : name_(name)
        , size_(size)
        , isWrite_(isWrite)
        , isDirect_(isDirect)
        , fd_(-1) {

        ::printf("device %s size %zu isWrite %d isDirect %d\n",
                 name_.c_str(), size_, isWrite_, isDirect_);
        
        int flags = (isWrite_ ? O_RDWR : O_RDONLY);
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
        if (! isWrite_) { throw std::string("write is not permitted."); }
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
    bool isWrite() const { return isWrite_; }
};

class IoResponseBench
{
private:
    BlockDevice& dev_;
    size_t blockSize_;
    size_t nBlocks_;
    void* bufV_;
    char* buf_;
    std::queue<double>& rtQ_;
    bool isShowEachResponse_;

public:
    /**
     * @param dev block device.
     * @param bs block size.
     * @param nBlocks disk size as number of blocks.
     */
    IoResponseBench(BlockDevice& dev, size_t blockSize,
                    size_t nBlocks, std::queue<double>& rtQ,
                    bool isShowEachResponse)
        : dev_(dev)
        , blockSize_(blockSize)
        , nBlocks_(nBlocks)
        , rtQ_(rtQ)
        , isShowEachResponse_(isShowEachResponse) {

        ::printf("blockSize %zu nBlocks %zu isShowEachResponse %d\n",
                 blockSize_, nBlocks_, isShowEachResponse_);

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

        double begin, end;
        begin = getTime();
        double rt;
        double max = -1.0;
        double min = -1.0;
        double total = 0.0;
        
        for (size_t i = 0; i < n; i ++) {
            rt = execBlockIO();
            if (isShowEachResponse_) { rtQ_.push(rt); }
            updateRt(rt, max, min, total);
        }
        end = getTime();
        ::printf("total %.06f count %zu avg %.06f max %.06f min %.06f\n",
                 total, n, total / static_cast<double>(n), max, min);
    }
    void execNsecs(size_t n) {

        double begin, end;
        begin = getTime(); end = begin;

        double rt, max = -1.0, min = -1.0, total = 0.0;
        size_t count = 0;
        
        while (end - begin < static_cast<double>(n)) {

            rt = execBlockIO();
            if (isShowEachResponse_) { rtQ_.push(rt); }
            updateRt(rt, max, min, total);
            end = getTime();
            count ++;
        }
        ::printf("total %.06f count %zu avg %.06f max %.06f min %.06f\n",
                 total, count, total / static_cast<double>(count), max, min);
    }

    
    
private:
    int getRandomInt(int max) {

        double maxF = static_cast<double>(max);
        return static_cast<int>(maxF * (::rand() / (RAND_MAX + 1.0)));
    }
    /**
     * @return response time.
     */
    double execBlockIO() {
        
        double begin, end;
        begin = getTime();
        size_t oft = getRandomInt(nBlocks_) * blockSize_;
        if (dev_.isWrite()) {
            dev_.write(oft, blockSize_, buf_);
        } else {
            dev_.read(oft, blockSize_, buf_);
        }
        end = getTime();
        return end - begin;
    }
    void updateRt(const double& rt, double& max, double& min, double& total) {

        if (max < 0 || min < 0) {
            max = rt; min = rt;
        } else if (max < rt) {
            max = rt;
        } else if (min > rt) {
            min = rt;
        }
        total += rt;
    }
};

struct Options
{
    std::string programName;
    size_t diskSize;
    size_t blockSize;
    std::vector<std::string> args;
    bool isWrite;
    bool isShowEachResponse;
    
    size_t period;
    size_t count;

    Options(int argc, char* argv[])
        : diskSize(0)
        , blockSize(0)
        , args()
        , isWrite(false)
        , isShowEachResponse(false)
        , period(0)
        , count(0) {

        parse(argc, argv);
        
        if (args.size() != 1 || diskSize == 0 || blockSize == 0) {
            throw std::string("specify disksize (-s), blocksize (-b), and device.");
        }
        if (period == 0 && count == 0) {
            throw std::string("specify period (-p) or count (-c).");
        }
    }

    void parse(int argc, char* argv[]) {

        programName = argv[0];
        
        while (1) {
            int c = ::getopt(argc, argv, "s:b:p:c:wrh");

            if (c < 0) { break; }

            switch (c) {
            case 's': /* disk size in blocks */
                diskSize = ::atol(optarg);
                break;
            case 'b': /* blocksize */
                blockSize = ::atol(optarg);
                break;
            case 'p': /* period */
                period = ::atol(optarg);
                break;
            case 'c': /* count */
                count = ::atol(optarg);
                break;
            case 'w': /* write */
                isWrite = true;
                break;
            case 'r': /* show each response */
                isShowEachResponse = true;
                break;
            case 'h': /* help */
                showHelp();
                break;
            }
        }

        while (optind < argc) {
            args.push_back(argv[optind ++]);
        }
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
                 "    -r:      show response of each IO.\n"
                 "    -h:      show this help.\n"
                 , programName.c_str()
            );
    }
};

int main(int argc, char* argv[])
{
    ::srand(::time(0) + ::getpid());
    std::queue<double> rtQ;

    try {
        Options opt(argc, argv);
        const size_t sizeInBytes = opt.diskSize * opt.blockSize;
        const bool isDirect = true;
        
        BlockDevice bd(opt.args[0], sizeInBytes, opt.isWrite, isDirect);
        IoResponseBench bench(bd, opt.blockSize, opt.diskSize,
                              rtQ, opt.isShowEachResponse);
        if (opt.period > 0) {
            bench.execNsecs(opt.period);
        } else {
            bench.execNtimes(opt.count);
        }

        while (! rtQ.empty()) {

            ::printf("response %.06f\n", rtQ.front());
            rtQ.pop();
        }
    } catch (const std::string& e) {

        ::printf("error: %s\n", e.c_str());
    }
    
    return 0;
}
